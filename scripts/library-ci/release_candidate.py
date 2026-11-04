"""Validated original identity shared by staging, recovery and publication."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from library_ci import CheckError, run
from packaging.utils import (
    canonicalize_name,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import Version

MARKER = "library-release-v1"
METADATA = "<!-- library-release-candidate:"


@dataclass(frozen=True)
class Transport:
    run_id: str
    attempt: str


@dataclass(frozen=True)
class Candidate:
    repository: str
    package: str
    version: str
    source_sha: str
    release_sha: str
    tag: str
    baseline: str
    baseline_version: str
    index: str
    artifacts: dict[str, str]
    retained: dict[str, str]
    transport: Transport
    original: bytes

    @classmethod
    def parse(cls, original, repository):
        try:
            manifest = json.loads(original)
            validate_manifest(manifest, repository)
        except (KeyError, TypeError, ValueError) as error:
            raise CheckError("Invalid retained candidate metadata.") from error
        return cls(
            repository=manifest["repository"],
            package=manifest["package"],
            version=manifest["version"],
            source_sha=manifest["source_sha"],
            release_sha=manifest["release_sha"],
            tag=manifest["tag"],
            baseline=manifest["baseline"],
            baseline_version=manifest["baseline_version"],
            index=manifest["index"],
            artifacts=manifest["artifacts"],
            retained=manifest["retained"],
            transport=Transport(
                manifest["transport"]["run_id"], manifest["transport"]["attempt"]
            ),
            original=original,
        )

    @property
    def prerelease(self):
        return Version(self.version).is_prerelease

    @property
    def tag_message(self):
        return MARKER + "\n" + hashlib.sha256(self.original).hexdigest()

    @property
    def hashes(self):
        return {
            "manifest.json": hashlib.sha256(self.original).hexdigest(),
            **self.artifacts,
            **self.retained,
        }

    def draft_body(self, notes):
        encoded = base64.b64encode(self.original).decode()
        return notes + "\n" + METADATA + encoded + " -->"

    def verify(self, name, data):
        if hashlib.sha256(data).hexdigest() != self.hashes[name]:
            raise CheckError("Retained original SHA256 differs; do not rebuild.")


def draft_candidate(release, repository):
    body = release.get("body") or ""
    if METADATA not in body:
        raise CheckError(
            "Required originals are missing candidate metadata; restore originals."
        )
    try:
        notes, encoded = body.rsplit("\n" + METADATA, 1)
        encoded = encoded.removesuffix(" -->")
        candidate = Candidate.parse(
            base64.b64decode(encoded, validate=True), repository
        )
    except ValueError as error:
        raise CheckError("Invalid retained candidate metadata.") from error
    if candidate.tag != release["tag_name"]:
        raise CheckError("Release tag differs from retained identity.")
    candidate.verify("notes.md", notes.encode())
    return candidate


def validate_transport(transport):
    if set(transport) != {"run_id", "attempt"} or not all(
        isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value)
        for value in transport.values()
    ):
        raise CheckError("Invalid original workflow transport identity.")


def validate_manifest(manifest, repository):
    if manifest["schema"] != MARKER or manifest["repository"] != repository:
        raise CheckError("Unexpected release manifest identity.")
    if not re.fullmatch(r"[0-9a-f]{40}", manifest["source_sha"]) or not re.fullmatch(
        r"[0-9a-f]{40}", manifest["release_sha"]
    ):
        raise CheckError("Invalid release commit identity.")
    if run("git", "check-ref-format", f"refs/tags/{manifest['tag']}").returncode:
        raise CheckError("Invalid release tag.")
    artifacts = manifest["artifacts"]
    if (
        len(artifacts) != 2
        or sum(name.endswith(".whl") for name in artifacts) != 1
        or sum(name.endswith(".tar.gz") for name in artifacts) != 1
    ):
        raise CheckError("Original wheel and sdist are required.")
    if set(manifest["retained"]) != {"notes.md", "release.bundle"}:
        raise CheckError("Original notes and prepared commit are required.")
    for name, expected in (artifacts | manifest["retained"]).items():
        if Path(name).name != name or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise CheckError("Invalid retained original name or SHA256.")
    validate_transport(manifest["transport"])
    index = urlsplit(manifest["index"])
    if (
        index.hostname != "europe-north1-python.pkg.dev"
        or index.path.rstrip("/") != "/fyndiq/qlty/simple"
    ):
        raise CheckError("Invalid candidate registry identity.")
    if canonicalize_name(manifest["package"], validate=True) != manifest["package"]:
        raise CheckError("Invalid normalized package identity.")
    if run("git", "check-ref-format", f"refs/tags/{manifest['baseline']}").returncode:
        raise CheckError("Invalid stable baseline tag.")
    Version(manifest["version"])
    Version(manifest["baseline_version"])
    for name in artifacts:
        parsed = (
            parse_wheel_filename(name)
            if name.endswith(".whl")
            else parse_sdist_filename(name)
        )
        if parsed[0] != manifest["package"] or parsed[1] != Version(
            manifest["version"]
        ):
            raise CheckError("Distribution filename differs from candidate identity.")
