"""Exact-original Actions transport and durable preparation evidence."""

import json
import os
import re
import zipfile
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path

from library_ci import CheckError
from packaging.version import Version
from release_candidate import Candidate
from release_context import GitHub


@dataclass(frozen=True)
class Allocation:
    repository: str
    run_id: str
    source_sha: str
    version: str

    @classmethod
    def parse(cls, data, repository, run_id):
        try:
            value = cls(**json.loads(data))
            if (
                value.repository != repository
                or value.run_id != run_id
                or not re.fullmatch(r"[0-9a-f]{40}", value.source_sha)
                or not Version(value.version).is_prerelease
                or str(Version(value.version)) != value.version
            ):
                raise ValueError
            return value
        except (TypeError, ValueError):
            raise CheckError("Invalid original allocation identity.") from None

    def write(self):
        directory = Path(os.environ["RUNNER_TEMP"]) / "library-allocation"
        directory.mkdir(exist_ok=True)
        (directory / "allocation.json").write_text(json.dumps(asdict(self)) + "\n")


class RecoveryStore:
    def __init__(self, repository):
        self.repository = repository
        self.api = None
        self.runs = {}

    def artifacts(self, run_id):
        if self.api is None:
            self.api = GitHub(self.repository, os.environ["DISPATCH_TOKEN"])
        if run_id not in self.runs:
            artifacts = []
            page = 1
            while True:
                batch = self.api.request(
                    "GET", f"actions/runs/{run_id}/artifacts?per_page=100&page={page}"
                )["artifacts"]
                artifacts.extend(batch)
                if len(batch) < 100:
                    break
                page += 1
            self.runs[run_id] = artifacts
        return self.runs[run_id]

    def artifact(self, run_id, name, required=False):
        matches = [a for a in self.artifacts(run_id) if a["name"] == name]
        if not matches and not required:
            return None
        if len(matches) != 1 or matches[0]["expired"]:
            raise CheckError(
                "Original workflow artifact is unavailable; restore originals without rebuilding."
            )
        data = self.api.request(
            "GET", f"actions/artifacts/{matches[0]['id']}/zip", raw=True
        )
        try:
            with zipfile.ZipFile(BytesIO(data)) as archive:
                names = archive.namelist()
                if len(set(names)) != len(names):
                    raise ValueError
                return {name: archive.read(name) for name in names}
        except (ValueError, zipfile.BadZipFile):
            raise CheckError("Invalid original workflow archive.") from None

    def candidate(self, run_id, attempt=None):
        if attempt is None:
            matches = [
                a
                for a in self.artifacts(run_id)
                if re.fullmatch(r"library-candidate-[1-9][0-9]*", a["name"])
            ]
            if not matches:
                return None
            if len(matches) != 1:
                raise CheckError(
                    "Original candidate artifact is ambiguous; restore originals."
                )
            attempt = matches[0]["name"].removeprefix("library-candidate-")
        originals = self.artifact(run_id, f"library-candidate-{attempt}", required=True)
        candidate = Candidate.parse(
            originals.get("manifest.json", b""), self.repository
        )
        if set(originals) != set(candidate.hashes):
            raise CheckError("Original candidate archive paths differ.")
        for name, data in originals.items():
            candidate.verify(name, data)
        if (
            candidate.transport.run_id != run_id
            or candidate.transport.attempt != attempt
        ):
            raise CheckError("Original candidate transport differs.")
        return candidate, originals

    def allocation(self, run_id):
        original = self.artifact(run_id, "library-allocation")
        if original is None:
            return None
        if set(original) != {"allocation.json"}:
            raise CheckError("Original allocation archive paths differ.")
        return Allocation.parse(original["allocation.json"], self.repository, run_id)

    def require_unprepared(self, run_id, attempt, require_uninspected=False):
        for previous in range(1, int(attempt)):
            original = self.artifact(
                run_id, f"library-preparation-{previous}", required=True
            )
            try:
                receipt = json.loads(original["preparation.json"])
                identity = {
                    key: receipt[key] for key in ("repository", "run_id", "attempt")
                }
                valid_identity = identity == {
                    "repository": self.repository,
                    "run_id": run_id,
                    "attempt": str(previous),
                }
                safe_preparation = receipt["preparation"] in {"skipped", "no-increment"}
                safe_inspection = receipt["inspection"] in (
                    {"skipped"} if require_uninspected else {"success", "skipped"}
                ) or (
                    not require_uninspected
                    and receipt["preparation"] == "skipped"
                    and receipt["inspection"] in {"failure", "cancelled"}
                )
                valid = (
                    set(original) == {"preparation.json"}
                    and set(receipt)
                    == {"repository", "run_id", "attempt", "preparation", "inspection"}
                    and valid_identity
                    and safe_preparation
                    and safe_inspection
                )
            except (KeyError, TypeError, ValueError):
                valid = False
            if not valid:
                raise CheckError(
                    "Prior preparation may have produced originals. Rerun all jobs to select a retained candidate, or restore originals without rebuilding."
                )


def preparation_receipt(repository, result, inspection, version="", mode=""):
    if result == "success" and not version and mode == "stable":
        result = "no-increment"
    if result not in {"success", "failure", "cancelled", "skipped", "no-increment"}:
        raise CheckError("Invalid preparation result.")
    directory = Path(os.environ["RUNNER_TEMP"]) / "library-preparation"
    directory.mkdir(exist_ok=True)
    (directory / "preparation.json").write_text(
        json.dumps(
            {
                "repository": repository,
                "run_id": os.environ["GITHUB_RUN_ID"],
                "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
                "preparation": result,
                "inspection": inspection,
            }
        )
        + "\n"
    )
