"""Prepare once, retain before tagging, and resume publication of library releases."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path
from urllib.parse import quote

from commitizen.config import read_cfg
from commitizen.providers import UvProvider
from commitizen.tags import TagRules
from library_ci import CheckError, git_output, preview, resolve, run
from packaging.utils import (
    canonicalize_name,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import Version
from release_candidate import (
    MARKER,
    Candidate,
    validate_transport,
)
from release_context import (
    GitHub,
    Registries,
    metadata_baseline,
    metadata_tag_rules,
    project,
    reconcile_registries,
    reconcile_registry,
    remote,
    retained_candidate,
    verify_baseline,
    verify_existing_tag,
    wait_for_registry,
)
from release_notes import PullRequests, release_section, updated_changelog
from release_plan import ReleaseRequest, pending_releases, plan
from release_recovery import RecoveryStore, preparation_receipt


def checked(*args):
    result = run(*args)
    if result.returncode:
        raise CheckError(f"{args[0]} {args[1]} failed (exit {result.returncode}).")
    return result.stdout.strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def outputs(**values):
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as stream:
            stream.writelines(
                f"{key.replace('_', '-')}={value}\n" for key, value in values.items()
            )


def inspect(github, resume_tag, branch, source_ref="", target_version="", phase=""):
    inspection = plan(
        github, ReleaseRequest(resume_tag, source_ref, target_version, phase)
    )
    outputs(**inspection.outputs())
    return inspection


def validate_distribution(path, package, version):
    if path.name.endswith(".whl"):
        parsed = parse_wheel_filename(path.name)
        with zipfile.ZipFile(path) as archive:
            if archive.testzip():
                raise CheckError("Wheel content failed its archive checksum.")
            names = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(names) != 1:
                raise CheckError(
                    "Wheel must contain exactly one package METADATA file."
                )
            metadata = archive.read(names[0])
    else:
        parsed = parse_sdist_filename(path.name)
        with tarfile.open(path) as archive:
            names = [
                item
                for item in archive
                if item.name.endswith("/PKG-INFO") and item.name.count("/") == 1
            ]
            if len(names) != 1:
                raise CheckError("Source distribution must contain package PKG-INFO.")
            metadata = archive.extractfile(names[0]).read()
    headers = BytesParser().parsebytes(metadata)
    if (
        str(parsed[0]) != package
        or str(parsed[1]) != version
        or canonicalize_name(headers["Name"]) != package
        or str(Version(headers["Version"])) != version
    ):
        raise CheckError("Distribution metadata does not match the release.")


def prepare(
    paths,
    directory,
    github,
    version="",
    source_sha="",
    mode="stable",
    inspected_attempt="",
):
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    if inspected_attempt != attempt and attempt != "1":
        RecoveryStore(github.repository).require_unprepared(
            os.environ["GITHUB_RUN_ID"], attempt
        )
    if mode not in {"stable", "prerelease"} or (mode == "stable" and version):
        raise CheckError("Invalid preparation mode or allocation.")
    if source_sha and resolve("HEAD") != source_sha:
        raise CheckError("Preparation source differs from the inspected commit.")
    transport = {
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
    }
    if not paths or directory.is_relative_to(
        Path(git_output("rev-parse", "--show-toplevel"))
    ):
        raise CheckError(
            "Configure packaged-content paths and an output directory outside the checkout."
        )
    if git_output("status", "--porcelain", "--untracked-files=no"):
        raise CheckError("Preparation requires clean tracked files.")
    _metadata, index = project()
    if read_cfg("pyproject.toml").settings.get("version_provider") != "uv":
        raise CheckError(
            "Release preparation requires Commitizen's uv version provider."
        )
    release = preview(paths)
    # Historical merge envelopes may be nonconventional; validate all ordinary commits.
    if resolve(release["baseline"]) != resolve("HEAD"):
        preview(paths, release["baseline"], allow_historical_merges=True)
    if mode == "prerelease":
        parsed = Version(version)
        if (
            not parsed.is_prerelease
            or parsed.is_devrelease
            or parsed.local
            or parsed.post
        ):
            raise CheckError("Expected an allocated prerelease version.")
        if parsed <= Version(release["current-version"]):
            raise CheckError("Prerelease must be newer than the stable baseline.")
        release["next-version"] = version
    outputs(next_version=release["next-version"])
    if not release["next-version"]:
        return
    validate_transport(transport)
    directory.mkdir(parents=True, exist_ok=False)
    source = resolve("HEAD")
    section = release_section(release, PullRequests(github.repository, github.token))
    (directory / "notes.md").write_text(section, encoding="utf-8")
    config = read_cfg("pyproject.toml")
    tag = TagRules.from_settings(config.settings).normalize_tag(release["next-version"])
    if run("git", "show-ref", "--verify", f"refs/tags/{tag}").returncode == 0:
        raise CheckError("Version tag already exists; resume it rather than bumping.")
    if mode == "prerelease":
        git_output("checkout", "--detach", source)
    UvProvider(config).set_version(release["next-version"])
    updated_lock = tomllib.loads(Path("uv.lock").read_text())
    project_entries = [
        p
        for p in updated_lock["package"]
        if canonicalize_name(p["name"]) == release["package"]
    ]
    if (
        len(project_entries) != 1
        or project_entries[0]["version"] != release["next-version"]
    ):
        raise CheckError(
            "Commitizen did not update exactly one matching uv.lock project entry."
        )
    if mode == "stable":
        changelog = Path("CHANGELOG.md")
        changelog.write_text(
            updated_changelog(
                section, changelog.read_text() if changelog.exists() else ""
            )
        )
        git_output("add", "CHANGELOG.md")
    git_output("add", "pyproject.toml", "uv.lock")
    checked(
        "git",
        "-c",
        "user.name=Library release",
        "-c",
        "user.email=release@fyndiq.com",
        "-c",
        "core.hooksPath=/dev/null",
        "commit",
        "-m",
        f"chore: release {release['next-version']}",
    )
    sha = resolve("HEAD")
    checked("uv", "build", "--out-dir", str(directory))
    artifacts = [
        path for path in directory.iterdir() if path.name.endswith((".whl", ".tar.gz"))
    ]
    if len(artifacts) != 2 or sum(path.suffix == ".whl" for path in artifacts) != 1:
        raise CheckError("Build must produce one wheel and one source distribution.")
    for path in artifacts:
        validate_distribution(path, release["package"], release["next-version"])
    if git_output("status", "--porcelain", "--untracked-files=no"):
        raise CheckError("Build changed tracked source files.")
    manifest = {
        "schema": MARKER,
        "notification": "library-slack-v1",
        "repository": github.repository,
        "package": release["package"],
        "version": release["next-version"],
        "source_sha": source,
        "release_sha": sha,
        "tag": tag,
        "baseline": release["baseline"],
        "baseline_version": release["current-version"],
        "index": index,
        "transport": transport,
        "artifacts": {path.name: digest(path) for path in artifacts},
    }
    # Bundle keeps the prepared commit recoverable even before the atomic push.
    checked("git", "update-ref", "refs/library-release/candidate", sha)
    checked(
        "git",
        "bundle",
        "create",
        str(directory / "release.bundle"),
        "refs/library-release/candidate",
        f"^{release['baseline'] if mode == 'prerelease' else source}",
    )
    manifest["retained"] = {
        name: digest(directory / name) for name in ("notes.md", "release.bundle")
    }
    (directory / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n"
    )
    outputs(candidate_attempt=transport["attempt"])


def load(directory, repository):
    candidate = Candidate.parse((directory / "manifest.json").read_bytes(), repository)
    for name in candidate.hashes:
        candidate.verify(name, (directory / name).read_bytes())
    return candidate


def retain(github, release, directory, candidate):
    existing = {asset["name"]: asset for asset in github.assets(release)}
    for name in candidate.hashes:
        path = directory / name
        if name in existing:
            if github.download(existing[name]) != path.read_bytes():
                raise CheckError(
                    "Existing release asset differs; never replace original assets."
                )
        else:
            github.upload(release, path)
    # Read back every original before making any Git or registry publication.
    retained = github.assets(release)
    required = set(candidate.hashes)
    if not required.issubset({asset["name"] for asset in retained}):
        raise CheckError(
            "Draft assets are incomplete; resume original staging before pushing."
        )
    for asset in retained:
        path = directory / asset["name"]
        if path.exists() and github.download(asset) != path.read_bytes():
            raise CheckError("Retained asset verification failed.")


def recover(github, release, directory, candidate):
    directory.mkdir(parents=True, exist_ok=True)
    assets = {asset["name"]: asset for asset in github.assets(release)}
    tagged = verify_existing_tag(candidate)
    if not release.get("draft", True) and not tagged:
        raise CheckError("Completed release is missing its original annotated tag.")
    missing = set(candidate.hashes) - assets.keys()
    # Validate every available original before any fallback transport request.
    for name in candidate.hashes.keys() & assets.keys():
        data = github.download(assets[name])
        candidate.verify(name, data)
        (directory / name).write_bytes(data)
    if missing:
        recovered, originals = RecoveryStore(github.repository).candidate(
            candidate.transport.run_id, candidate.transport.attempt
        )
        if recovered.original != candidate.original:
            raise CheckError("Candidate differs from the original retained release.")
        for name in missing:
            (directory / name).write_bytes(originals[name])
        retain(github, release, directory, candidate)
    return candidate


def dispatch_current(github, branch, retry):
    workflow = (
        os.environ["GITHUB_WORKFLOW_REF"]
        .split("@", 1)[0]
        .split("/.github/workflows/", 1)[1]
    )
    dispatch = GitHub(github.repository, os.environ["DISPATCH_TOKEN"])
    dispatch.request(
        "POST",
        f"actions/workflows/{quote(workflow, safe='')}/dispatches",
        {"ref": branch, "inputs": {"resume_tag": "", "release_retry": str(retry)}},
    )


def stale_restart(github, release, candidate, registry, branch, retry):
    if remote(f"refs/tags/{candidate.tag}") or registry.files(
        candidate.package, candidate.version
    ):
        raise CheckError(
            "Stale candidate has publication evidence; reconcile manually without changing tags."
        )
    if retry >= 3:
        raise CheckError("Source advanced repeatedly; rerun after merges settle.")
    github.request("DELETE", f"releases/{release['id']}")
    dispatch_current(github, branch, retry + 1)
    outputs(restarted="true")


def verify_prerelease_snapshot(candidate):
    relative = Path.cwd().relative_to(Path(git_output("rev-parse", "--show-toplevel")))
    paths = {str(relative / "pyproject.toml"), str(relative / "uv.lock")}
    changed = git_output(
        "diff", "--name-only", candidate.source_sha, candidate.release_sha
    ).splitlines()
    if set(changed) != paths:
        raise CheckError(
            "Prerelease snapshot must change only project version and lock."
        )

    def metadata(sha, name):
        return tomllib.loads(git_output("show", f"{sha}:{relative / name}"))

    original = metadata(candidate.source_sha, "pyproject.toml")
    prepared = metadata(candidate.release_sha, "pyproject.toml")
    baseline = original["project"]["version"]
    rules = metadata_tag_rules(original)
    if (
        candidate.package != canonicalize_name(original["project"]["name"])
        or candidate.baseline_version != baseline
        or candidate.baseline != metadata_baseline(original)
        or candidate.tag != rules.normalize_tag(candidate.version)
        or Version(candidate.version) <= Version(baseline)
        or prepared["project"]["version"] != candidate.version
    ):
        raise CheckError("Prerelease snapshot differs from the source identity.")
    prepared["project"]["version"] = baseline
    if prepared != original:
        raise CheckError("Prerelease snapshot changed project configuration.")
    lock = metadata(candidate.source_sha, "uv.lock")
    prepared_lock = metadata(candidate.release_sha, "uv.lock")
    entries = [
        p
        for p in prepared_lock["package"]
        if canonicalize_name(p["name"]) == candidate.package
    ]
    if len(entries) != 1 or entries[0]["version"] != candidate.version:
        raise CheckError("Prerelease snapshot has an invalid project lock entry.")
    entries[0]["version"] = baseline
    if prepared_lock != lock:
        raise CheckError("Prerelease snapshot changed locked dependencies.")


def reconcile_git(candidate, directory, branch, has_registry_files):
    if verify_existing_tag(candidate):
        return True
    if has_registry_files:
        raise CheckError(
            "Registry files exist without the original tag; reconcile manually."
        )
    if (
        not candidate.prerelease
        and remote(f"refs/heads/{branch}") != candidate.source_sha
    ):
        return False
    # Prerelease bundles include the selected source history above the stable baseline.
    if not candidate.prerelease:
        git_output("fetch", "origin", candidate.source_sha)
    checked("git", "bundle", "verify", str(directory / "release.bundle"))
    git_output(
        "fetch", str(directory / "release.bundle"), "refs/library-release/candidate"
    )
    if (
        resolve("FETCH_HEAD") != candidate.release_sha
        or git_output("rev-parse", f"{candidate.release_sha}^") != candidate.source_sha
    ):
        raise CheckError(
            "Prepared commit does not descend directly from the tested source."
        )
    if candidate.prerelease:
        verify_prerelease_snapshot(candidate)
    local_tag = f"refs/tags/{candidate.tag}"
    if run("git", "show-ref", "--verify", local_tag).returncode == 0:
        if (
            resolve(local_tag + "^{}") != candidate.release_sha
            or git_output("for-each-ref", "--format=%(contents)", local_tag)
            != candidate.tag_message
        ):
            raise CheckError("Local tag differs from the original release candidate.")
    else:
        checked(
            "git",
            "-c",
            "user.name=Library release",
            "-c",
            "user.email=release@fyndiq.com",
            "tag",
            "-a",
            candidate.tag,
            candidate.release_sha,
            "-m",
            candidate.tag_message,
        )
    if candidate.prerelease:
        pushed = run("git", "push", "origin", f"{local_tag}:{local_tag}")
        if pushed.returncode:
            raise CheckError("Tag push failed; resume the retained release.")
        return True
    pushed = run(
        "git",
        "push",
        "--atomic",
        f"--force-with-lease=refs/heads/{branch}:{candidate.source_sha}",
        "origin",
        f"{candidate.release_sha}:refs/heads/{branch}",
        f"refs/tags/{candidate.tag}:refs/tags/{candidate.tag}",
    )
    if pushed.returncode:
        if (
            not remote(f"refs/tags/{candidate.tag}")
            and remote(f"refs/heads/{branch}") != candidate.source_sha
        ):
            return False
        raise CheckError("Atomic push failed; resume the retained release.")
    return True


def publish(
    github,
    directory,
    resume_tag,
    branch,
    retry,
    registry_factory=Registries,
    source_sha="",
    version="",
    followup_stable=False,
):
    if not 0 <= retry <= 3:
        raise CheckError("Internal retry counter must be between zero and three.")
    releases = github.releases()
    pending = pending_releases(releases)
    if len(pending) > 1:
        raise CheckError("Multiple pending releases require reconciliation.")
    candidate = None if resume_tag else load(directory, github.repository)
    if candidate and (
        (source_sha and candidate.source_sha != source_sha)
        or (version and candidate.version != version)
    ):
        raise CheckError(
            "Prepared candidate differs from the trusted inspected identity."
        )
    if (
        candidate
        and source_sha
        and bool(version and Version(version).is_prerelease) != candidate.prerelease
    ):
        raise CheckError("Prepared release mode differs from the inspected identity.")
    tag = resume_tag or candidate.tag
    matches = [r for r in releases if r["tag_name"] == tag]
    if pending and (not matches or pending[0]["id"] != matches[0]["id"]):
        raise CheckError("Resume the pending release before preparing another version.")
    if len(matches) > 1:
        raise CheckError("Ambiguous retained release identity.")
    if matches:
        release = matches[0]
        original = retained_candidate(github, release)
        if candidate and candidate.original != original.original:
            raise CheckError("Candidate differs from the original retained release.")
        candidate = recover(github, release, directory, original)
    else:
        if resume_tag:
            raise CheckError("Original retained release is missing; do not rebuild.")
        if remote(f"refs/tags/{tag}"):
            raise CheckError("Release tag collision; never move the existing tag.")
        registry = registry_factory(candidate.index)
        if registry.files(candidate.package, candidate.version):
            raise CheckError(
                "Version already exists in the registry; reconcile rather than bump."
            )
        release = github.request(
            "POST",
            "releases",
            {
                "tag_name": tag,
                "target_commitish": candidate.source_sha,
                "name": candidate.version,
                "body": candidate.draft_body((directory / "notes.md").read_text()),
                "draft": True,
                "prerelease": candidate.prerelease,
            },
        )
        retain(github, release, directory, candidate)
    if bool(release.get("prerelease", False)) != candidate.prerelease:
        raise CheckError(
            "GitHub Release prerelease state differs from the retained version."
        )
    notes = (directory / "notes.md").read_text()
    expected_body = candidate.draft_body(notes) if release["draft"] else notes
    if candidate.tag != release["tag_name"] or release["body"] != expected_body:
        raise CheckError("Release notes or tag differ from retained identity.")
    registry = registry_factory(candidate.index)
    # A completed stable baseline must have both distributions, including legacy
    # releases without a GitHub Release. A tag alone never establishes completion.
    verify_baseline(
        github,
        registry,
        candidate.package,
        candidate.baseline_version,
        candidate.baseline,
        releases,
    )
    found = reconcile_registries(registry, candidate)
    if not reconcile_git(candidate, directory, branch, bool(found)):
        stale_restart(github, release, candidate, registry, branch, retry)
        return
    qlty = registry.qlty
    qlty_found = reconcile_registry(qlty, candidate, label="qlty")
    if qlty_found != candidate.artifacts:
        # The trusted publisher never executes source code or its build backend.
        config = directory / "uv-publish.toml"
        config.write_text(
            '[[index]]\nname = "qlty"\nurl = "https://oauth2accesstoken@europe-north1-python.pkg.dev/fyndiq/qlty/simple/"\npublish-url = "https://europe-north1-python.pkg.dev/fyndiq/qlty/"\n'
        )
        checked(
            "uv",
            "publish",
            "--config-file",
            str(config),
            "--index",
            "qlty",
            "--keyring-provider",
            "subprocess",
            "--username",
            "oauth2accesstoken",
            *(str(directory / name) for name in candidate.artifacts),
        )
        wait_for_registry(qlty, candidate, "qlty")
    publish_pypi(registry.pypi, candidate, directory)
    reconcile_registries(registry, candidate, complete=True)
    if release["draft"]:
        release = github.request(
            "PATCH",
            f"releases/{release['id']}",
            {
                "draft": False,
                "prerelease": candidate.prerelease,
                "name": candidate.version,
                "body": (directory / "notes.md").read_text(),
            },
        )
    outputs(
        completed="true",
        prerelease=str(candidate.prerelease).lower(),
        tag=candidate.tag,
        version=candidate.version,
        package=candidate.package,
        release_url=release["html_url"],
        release_id=release["id"],
        notes_asset="notes.md",
        manifest_asset="manifest.json",
    )
    if followup_stable and remote(f"refs/heads/{branch}") != candidate.release_sha:
        dispatch_current(github, branch, 0)


def publish_pypi(registry, candidate, directory):
    found = reconcile_registry(registry, candidate, label="PyPI")
    if found == candidate.artifacts:
        return
    token = os.environ.get("PYPI_API_TOKEN")
    if not token:
        raise CheckError("PyPI token is unavailable; resume the same retained tag.")
    # Only this child receives the token. Never put it in arguments or diagnostics.
    environment = os.environ.copy()
    environment.pop("PYPI_API_TOKEN", None)
    environment["UV_PUBLISH_TOKEN"] = token
    result = subprocess.run(
        [
            "uv",
            "publish",
            "--no-config",
            "--publish-url",
            "https://upload.pypi.org/legacy/",
            "--check-url",
            "https://pypi.org/simple/",
            *(str(directory / name) for name in candidate.artifacts),
        ],
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )
    if result.returncode:
        raise CheckError("PyPI publication failed; resume the same retained tag.")
    wait_for_registry(registry, candidate, "PyPI")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["inspect", "prepare", "publish", "receipt"])
    parser.add_argument("--working-directory", default=".")
    parser.add_argument("--directory", default="")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--branch", default="master")
    parser.add_argument("--resume-tag", default="")
    parser.add_argument("--release-paths", default="")
    parser.add_argument("--source-ref", default="")
    parser.add_argument("--target-version", default="")
    parser.add_argument("--phase", default="")
    parser.add_argument("--source-sha", default="")
    parser.add_argument("--version", default="")
    parser.add_argument("--followup-stable", choices=["true", "false"], default="false")
    parser.add_argument("--inspected-attempt", default="")
    parser.add_argument("--mode", default="")
    parser.add_argument("--preparation-result", default="")
    parser.add_argument("--inspection-result", default="")
    parser.add_argument("--retry", type=int, default=0)
    args = parser.parse_args()
    try:
        os.chdir(args.working_directory)
        if args.command == "receipt":
            preparation_receipt(
                args.repository,
                args.preparation_result,
                args.inspection_result,
                args.version,
                args.mode,
            )
            return 0
        github = GitHub(args.repository)
        directory = Path(args.directory).resolve()
        if args.command == "inspect":
            inspect(
                github,
                args.resume_tag,
                args.branch,
                args.source_ref,
                args.target_version,
                args.phase,
            )
        elif args.command == "prepare":
            prepare(
                [p.strip() for p in args.release_paths.splitlines() if p.strip()],
                directory,
                github,
                args.version,
                args.source_sha,
                args.mode or "stable",
                args.inspected_attempt,
            )
        else:
            publish(
                github,
                directory,
                args.resume_tag,
                args.branch,
                args.retry,
                source_sha=args.source_sha,
                version=args.version,
                followup_stable=args.followup_stable == "true",
            )
    except CheckError as error:
        print(f"Library release failed: {error}", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001 - transport/backend errors can contain credentials
        # Suppress credential-bearing transport/backend diagnostics.
        print(
            "Library release failed: invalid metadata, missing originals, or unavailable tooling. Resume the same release after correcting the cause.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
