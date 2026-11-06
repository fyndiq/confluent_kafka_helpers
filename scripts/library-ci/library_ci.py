"""Read-only commit validation, distribution builds, and release previews."""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path

from commitizen import bump, git
from commitizen.config import read_cfg
from commitizen.cz.conventional_commits import ConventionalCommitsCz
from commitizen.exceptions import CommitizenException
from commitizen.tags import TagRules
from packaging.utils import canonicalize_name
from packaging.version import Version


class CheckError(Exception):
    pass


def run(*args: str) -> subprocess.CompletedProcess[str]:
    # Build backends and registry clients may include credentials in diagnostics.
    environment = os.environ.copy()
    environment.pop("PYPI_API_TOKEN", None)
    environment.pop("UV_PUBLISH_TOKEN", None)
    return subprocess.run(
        args, text=True, capture_output=True, check=False, env=environment
    )


def git_output(*args: str) -> str:
    result = run("git", *args)
    if result.returncode:
        raise CheckError(
            f"Git operation failed: {args[0]}. Fetch complete history and tags."
        )
    return result.stdout.strip()


def resolve(ref: str) -> str:
    return git_output("rev-parse", "--verify", f"{ref}^{{commit}}")


@dataclass(frozen=True)
class RawCommit:
    rev: str
    message: str
    parents: tuple[str, ...]


def commits(base: str, head: str) -> list[RawCommit]:
    fields = git_output(
        "log", "--reverse", "-z", "--format=%H%x00%P%x00%B", f"{base}..{head}"
    ).split("\0")[:-1]
    return [
        RawCommit(rev, message, tuple(parents.split()))
        for rev, parents, message in zip(
            fields[::3], fields[1::3], fields[2::3], strict=True
        )
    ]


def validate_commits(introduced: list[RawCommit], cz: ConventionalCommitsCz) -> None:
    invalid = [
        c.rev[:12]
        for c in introduced
        if not re.fullmatch(cz.schema_pattern(), c.message)
    ]
    if invalid:
        raise CheckError(
            "Invalid Conventional Commit messages: "
            + ", ".join(invalid)
            + ". Reword every introduced commit, keeping a blank line before the body."
        )


def packaged_changes(base: str, head: str, patterns: list[str]) -> bool:
    # Include both sides of renames and deletions, including removed packaged assets.
    names = git_output("diff", "--name-only", "--no-renames", "-z", base, head)
    return any(
        fnmatch.fnmatchcase(name, pattern)
        for name in names.split("\0")
        for pattern in patterns
    )


def preview(
    release_paths: list[str],
    base: str | None = None,
    head: str = "HEAD",
    *,
    allow_historical_merges: bool = False,
) -> dict[str, str]:
    if git_output("rev-parse", "--is-shallow-repository") != "false":
        raise CheckError(
            "Shallow history cannot produce a release preview. Use fetch-depth: 0."
        )
    head = resolve(head)
    if head != resolve("HEAD"):
        raise CheckError("Check out the exact source head before previewing.")
    metadata = tomllib.loads(Path("pyproject.toml").read_text())
    package = canonicalize_name(metadata["project"]["name"], validate=True)
    try:
        config = read_cfg("pyproject.toml")
        rules = TagRules.from_settings(config.settings)
        stable_tag = rules.find_tag_for(git.get_tags(), metadata["project"]["version"])
    except CommitizenException as error:
        raise CheckError(
            f"Commitizen preview failed (exit {error.exit_code}); check pyproject.toml and Git metadata."
        ) from None
    settings = config.settings
    if settings.get("name") != "cz_conventional_commits":
        raise CheckError("Configure tool.commitizen.name = 'cz_conventional_commits'.")
    if settings.get("version_provider") not in {"uv", "pep621"}:
        raise CheckError(
            "Use Commitizen's uv version provider (pep621 is accepted before migration)."
        )
    current = Version(metadata["project"]["version"])
    if current.is_prerelease or current.is_devrelease:
        raise CheckError(
            "A stable project version is required as the preview baseline."
        )
    baseline = stable_tag.name if stable_tag else rules.normalize_tag(str(current))
    stable_sha = resolve(f"refs/tags/{baseline}")
    if run("git", "merge-base", "--is-ancestor", stable_sha, head).returncode:
        raise CheckError(
            "The stable version tag must be an ancestor of the source head."
        )
    cz = ConventionalCommitsCz(config)
    if base:
        base = resolve(base)
        introduced = commits(base, head)
        if not introduced:
            raise CheckError("The PR range is empty. Check the supplied base and head.")
        validate_commits(
            [
                commit
                for commit in introduced
                if not allow_historical_merges or len(commit.parents) < 2
            ],
            cz,
        )
        comparison = git_output("merge-base", base, head)
        increment_map = (
            cz.bump_map_major_version_zero
            if settings.get("major_version_zero")
            else cz.bump_map
        )
        bump_commits = []
        for commit in introduced:
            title, _, body = commit.message.partition("\n\n")
            bump_commits.append(git.GitCommit(commit.rev, title, body))
        if packaged_changes(
            comparison, head, release_paths
        ) and not bump.find_increment(bump_commits, cz.bump_pattern, increment_map):
            raise CheckError(
                "Packaged content changed without a release-eligible commit. "
                "Use fix:, feat:, or a breaking-change marker for the package change."
            )
    changed = packaged_changes(stable_sha, head, release_paths)
    next_version = ""
    has_release_commits = bool(
        git_output("rev-list", "--max-count=1", f"{stable_sha}..{head}")
    )
    result = run(
        sys.executable,
        "-m",
        "commitizen",
        "--config",
        "pyproject.toml",
        "bump",
        "--get-next",
    )
    if result.returncode == 0:
        next_version = str(Version(result.stdout.strip()))
    elif result.returncode != 21 and not (
        result.returncode == 3 and not has_release_commits
    ):
        raise CheckError(
            f"Commitizen preview failed (exit {result.returncode}); no release was skipped."
        )
    if changed and not next_version:
        raise CheckError(
            "Packaged content changed since the stable baseline without a release-eligible commit."
        )
    return {
        "package": package,
        "current-version": str(current),
        "baseline": baseline,
        "next-version": next_version,
    }


def build() -> None:
    # Use the caller's build backend. Keep distributions outside the source tree.
    with tempfile.TemporaryDirectory(prefix="library-build-") as output:
        result = run("uv", "build", "--out-dir", output)
        if result.returncode:
            raise CheckError(
                f"Distribution build failed (exit {result.returncode}); inspect the backend locally."
            )
        files = list(Path(output).iterdir())
        if (
            len([p for p in files if p.suffix == ".whl"]) != 1
            or len([p for p in files if p.name.endswith(".tar.gz")]) != 1
        ):
            raise CheckError(
                "The build must produce one wheel and one source distribution."
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--working-directory", default=".")
    parser.add_argument(
        "--release-paths",
        required=True,
        help="Newline-separated repository-relative globs",
    )
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--skip-build", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    patterns = [
        line.strip() for line in args.release_paths.splitlines() if line.strip()
    ]
    try:
        if not patterns:
            raise CheckError(
                "Configure release_paths to cover all packaged content and build metadata."
            )
        os.chdir(args.working_directory)
        before = git_output("status", "--porcelain", "--untracked-files=no")
        if before:
            raise CheckError(
                "Tracked files must be clean before building and previewing."
            )
        result = preview(patterns, args.base or None, args.head)
        if not args.skip_build:
            build()
        if git_output("status", "--porcelain", "--untracked-files=no") != before:
            raise CheckError(
                "Build/preview changed tracked files. Configure a read-only build backend."
            )
        message = (
            f"{result['package']}: {result['current-version']} -> "
            + (result["next-version"] or "no release-eligible commits")
            + f" (stable baseline {result['baseline']})"
        )
        print(message)
        if output := os.environ.get("GITHUB_OUTPUT"):
            with open(output, "a") as stream:
                stream.writelines(f"{key}={value}\n" for key, value in result.items())
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(summary, "a") as stream:
                stream.write(message + "\n")
    except CheckError as error:
        # Do not emit raw subprocess diagnostics, package metadata, or commit bodies.
        print(f"Library CI failed: {error}", file=sys.stderr)
        return 1
    except (KeyError, ValueError, OSError):
        print(
            "Library CI failed: invalid or unavailable project metadata/tooling.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
