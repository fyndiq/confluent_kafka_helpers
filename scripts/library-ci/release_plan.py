"""Trusted inspection chooses one validated preparation or recovery outcome."""

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

from commitizen import git
from commitizen.config import read_cfg
from commitizen.tags import TagRules
from library_ci import CheckError, git_output, resolve
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version
from release_candidate import MARKER, METADATA, draft_candidate
from release_context import (
    Registries,
    metadata_baseline,
    metadata_tag_rules,
    project,
    verify_baseline,
)
from release_recovery import Allocation, RecoveryStore


@dataclass(frozen=True)
class ReleaseRequest:
    resume_tag: str = ""
    source_ref: str = ""
    target_version: str = ""
    phase: str = ""

    intent: str = field(init=False)

    def __post_init__(self):
        manual = prerelease_inputs(self.source_ref, self.target_version, self.phase)
        if self.resume_tag and manual:
            raise CheckError(
                "Resume with resume_tag alone; the original identity is retained."
            )
        object.__setattr__(
            self,
            "intent",
            "resume" if self.resume_tag else "prerelease" if manual else "stable",
        )


@dataclass(frozen=True)
class Inspection:
    outcome: str
    mode: str
    source_sha: str
    version: str = ""
    resume_tag: str = ""
    candidate_attempt: str = ""
    allocation_retained: bool = False
    followup_stable: bool = False

    def __post_init__(self):
        if (
            self.outcome not in {"prepare", "reuse", "resume"}
            or self.mode not in {"stable", "prerelease"}
            or not re.fullmatch(r"[0-9a-f]{40}", self.source_sha)
            or bool(self.resume_tag) != (self.outcome == "resume")
            or bool(self.candidate_attempt) != (self.outcome == "reuse")
            or (self.followup_stable and self.outcome != "resume")
            or (
                self.outcome != "resume"
                and self.mode == "prerelease"
                and not Version(self.version).is_prerelease
            )
        ):
            raise CheckError("Invalid inspected release outcome.")

    def outputs(self):
        return {
            "inspected_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
            "outcome": self.outcome,
            "mode": self.mode,
            "source_sha": self.source_sha,
            "next_version": self.version,
            "resume_tag": self.resume_tag,
            "candidate_attempt": self.candidate_attempt,
            "allocation_retained": str(self.allocation_retained).lower(),
            "followup_stable": str(self.followup_stable).lower(),
        }


def pending_releases(releases):
    return [r for r in releases if r["draft"] and METADATA in (r.get("body") or "")]


def prerelease_inputs(source_ref, target_version, phase):
    if not any((source_ref, target_version, phase)):
        return False
    if not source_ref or phase not in {"alpha", "beta", "rc"}:
        raise CheckError(
            "Prereleases require source_ref, target_version and alpha, beta, or rc phase."
        )
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", target_version):
        raise CheckError("target_version must be a stable major.minor.patch version.")
    if str(Version(target_version)) != target_version:
        raise CheckError("Use a normalized stable target_version.")
    return True


def allocate_prerelease(metadata, registry, target, phase):
    current = Version(metadata["project"]["version"])
    if current.is_prerelease or current.is_devrelease or Version(target) <= current:
        raise CheckError(
            "The stable target must be newer than the stable source version."
        )
    rules = metadata_tag_rules(metadata)
    marker = {"alpha": "a", "beta": "b", "rc": "rc"}[phase]
    used = registry.versions(canonicalize_name(metadata["project"]["name"]))
    tags = git.get_tags()
    for tag in tags:
        try:
            used.add(Version(str(rules.extract_version(tag))))
        except InvalidVersion:
            continue
    # Tags may exist before an upload; either record reserves its sequence number.
    sequences = [
        v.pre[1]
        for v in used
        if v.release == Version(target).release and v.pre and v.pre[0] == marker
    ]
    if Version(target) in used:
        raise CheckError("The target stable version already exists.")
    return target + marker + str(max(sequences, default=0) + 1)


def plan(github, request):
    resume_tag, source_ref, target_version, phase = (
        request.resume_tag,
        request.source_ref,
        request.target_version,
        request.phase,
    )
    manual = request.intent == "prerelease"
    store = RecoveryStore(github.repository)
    run_id = os.environ.get("GITHUB_RUN_ID")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    releases = github.releases()
    pending = pending_releases(releases)
    if len(pending) > 1:
        raise CheckError(
            "Multiple pending releases; reconcile retained originals before proceeding."
        )
    if resume_tag:
        matches = [r for r in releases if r["tag_name"] == resume_tag]
        if len(matches) != 1:
            raise CheckError("Resume requires the original retained GitHub Release.")
        if pending and pending[0]["id"] != matches[0]["id"]:
            raise CheckError("Resume the unfinished release before another tag.")
        tag = resume_tag
    else:
        tag = pending[0]["tag_name"] if pending else ""
    if tag:
        retained = next(r for r in releases if r["tag_name"] == tag)
        candidate = (
            draft_candidate(retained, github.repository) if retained["draft"] else None
        )
        prerelease = (
            candidate.prerelease if candidate else retained.get("prerelease", False)
        )
        return Inspection(
            "resume",
            "prerelease" if prerelease else "stable",
            resolve("HEAD"),
            resume_tag=tag,
            followup_stable=request.intent == "stable",
        )
    # Missing draft state for an automation tag is an integrity error, never
    # permission to allocate another version.
    for ref in git_output(
        "for-each-ref", "--format=%(refname)", "refs/tags"
    ).splitlines():
        if git_output("for-each-ref", "--format=%(contents)", ref).startswith(
            MARKER + "\n"
        ) and not any(
            r["tag_name"] == ref.removeprefix("refs/tags/") for r in releases
        ):
            raise CheckError(
                "Automation tag has lost its retained release; recover originals before proceeding."
            )
    previous = store.candidate(run_id) if run_id and attempt != "1" else None
    allocation = store.allocation(run_id) if run_id and attempt != "1" else None
    if previous:
        previous, _ = previous
        if previous.prerelease and (
            not allocation
            or previous.source_sha != allocation.source_sha
            or previous.version != allocation.version
        ):
            raise CheckError("Original candidate differs from its trusted allocation.")
        return Inspection(
            "reuse",
            "prerelease" if previous.prerelease else "stable",
            previous.source_sha,
            previous.version,
            candidate_attempt=previous.transport.attempt,
        )
    if run_id and attempt != "1":
        store.require_unprepared(
            run_id, attempt, require_uninspected=manual and allocation is None
        )
    if allocation:
        return Inspection(
            "prepare",
            "prerelease",
            allocation.source_sha,
            allocation.version,
            allocation_retained=True,
        )
    if manual:
        # Read metadata as data only; never check out selected code in this job.
        response = github.request("GET", "commits/" + quote(source_ref, safe=""))
        source = response["sha"]
        if not re.fullmatch(r"[0-9a-f]{40}", source):
            raise CheckError("Invalid resolved source commit.")
        git_output("fetch", "origin", source)
        relative = Path.cwd().relative_to(
            Path(git_output("rev-parse", "--show-toplevel"))
        )
        metadata = tomllib.loads(
            git_output("show", f"{source}:{relative / 'pyproject.toml'}")
        )
        indexes = [i for i in metadata["tool"]["uv"]["index"] if i["name"] == "qlty"]
        if len(indexes) != 1:
            raise CheckError("Configure exactly one uv index named qlty.")
        registry = Registries(indexes[0]["url"])
        stable = metadata_baseline(metadata)
        verify_baseline(
            github,
            registry,
            canonicalize_name(metadata["project"]["name"]),
            metadata["project"]["version"],
            stable,
            releases,
        )
        version = allocate_prerelease(metadata, registry, target_version, phase)
        Allocation(
            github.repository, os.environ["GITHUB_RUN_ID"], source, version
        ).write()
        return Inspection("prepare", "prerelease", source, version)
    metadata, index = project()
    config = read_cfg("pyproject.toml")
    stable = TagRules.from_settings(config.settings).normalize_tag(
        metadata["project"]["version"]
    )
    verify_baseline(
        github,
        Registries(index),
        canonicalize_name(metadata["project"]["name"]),
        metadata["project"]["version"],
        stable,
        releases,
    )
    return Inspection("prepare", "stable", resolve("HEAD"))
