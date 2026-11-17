"""Prepare one canonical release-note section without changing library files."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from http.client import HTTPException
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import URLError
from urllib.request import Request, urlopen

from commitizen import changelog, defaults, git
from commitizen.changelog_formats.markdown import Markdown
from commitizen.config import read_cfg
from commitizen.cz.conventional_commits import ConventionalCommitsCz
from commitizen.exceptions import CommitizenException
from commitizen.tags import TagRules
from jinja2 import Environment
from library_ci import CheckError, git_output, preview


class PullRequests:
    """Resolve changelog SHAs once per preparation, including unassociated commits."""

    def __init__(self, repository: str, token: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise CheckError("Use an owner/repository GitHub repository name.")
        if not token:
            raise CheckError("Set GH_TOKEN for commit-to-PR lookup.")
        self.repository = repository
        self.token = token
        self.cache: dict[str, str] = {}

    def link(self, sha: str) -> str:
        if sha in self.cache:
            return self.cache[sha]
        pulls = []
        page = 1
        while True:
            request = Request(
                f"https://api.github.com/repos/{self.repository}/commits/{sha}/pulls"
                f"?per_page=100&page={page}",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            try:
                with urlopen(request, timeout=30) as response:
                    batch = json.load(response)
                if not isinstance(batch, list):
                    raise TypeError
                for pull in batch:
                    if (
                        pull["merged_at"]
                        and pull["base"]["repo"]["full_name"].lower()
                        == self.repository.lower()
                    ):
                        if type(pull["number"]) is not int or pull["number"] <= 0:
                            raise ValueError
                        pulls.append(pull)
            except (
                URLError,
                OSError,
                HTTPException,
                ValueError,
                KeyError,
                TypeError,
                AttributeError,
            ):
                raise CheckError(
                    "GitHub commit-to-PR lookup failed; release notes were not prepared."
                ) from None
            if len(batch) < 100:
                break
            page += 1
        exact = [pull for pull in pulls if pull.get("merge_commit_sha") == sha]
        candidates = {pull["number"] for pull in exact or pulls}
        if len(candidates) > 1:
            raise CheckError(
                "Multiple merged PRs match a changelog commit; resolve the association."
            )
        root = f"https://github.com/{self.repository}"
        link = (
            f"[#{number}]({root}/pull/{number})"
            if (number := next(iter(candidates), None))
            else f"[{sha[:7]}]({root}/commit/{sha})"
        )
        self.cache[sha] = link
        return link


def release_section(release: dict[str, str], pulls: PullRequests) -> str:
    """Render only baseline..HEAD. Call preview first to validate the baseline/version."""
    if not release["next-version"]:
        raise CheckError("No release-eligible commits; no release notes were prepared.")
    config = read_cfg("pyproject.toml")
    cz = ConventionalCommitsCz(config)

    def enrich(message, commit):
        message["message"] = message["message"] or ""
        if message.get("breaking") or (
            message.get("change_type") is None
            and commit.title.partition(":")[0].endswith("!")
        ):
            message["message"] = "**BREAKING CHANGE**: " + message["message"]
        message["message"] += f" ({pulls.link(commit.rev)})"
        return message

    tree = changelog.generate_tree_from_commits(
        git.get_commits(start=release["baseline"], end="HEAD", args="--topo-order"),
        [],  # Intermediate prerelease tags must not split the new stable section.
        cz.commit_parser,
        cz.changelog_pattern,
        TagRules.from_settings(config.settings).normalize_tag(release["next-version"]),
        change_type_map=config.settings.get("change_type_map") or cz.change_type_map,
        changelog_message_builder_hook=enrich,
    )
    tree = list(
        changelog.generate_ordered_changelog_tree(
            tree,
            config.settings.get("change_type_order")
            or cz.change_type_order
            or defaults.CHANGE_TYPE_ORDER,
        )
    )
    for release_entry in tree:
        for group, messages in release_entry["changes"].items():
            # Compare rendered entries after Commitizen maps types into groups.
            unique = {}
            for message in messages:
                unique.setdefault((message.get("scope"), message["message"]), message)
            release_entry["changes"][group] = list(unique.values())
    # Load the pinned package template directly, ignoring caller template overrides.
    template = Environment(loader=cz.template_loader, trim_blocks=True).get_template(
        "CHANGELOG.md.j2"
    )
    return template.render(tree=tree).strip() + "\n"


def updated_changelog(section: str, existing: str) -> str:
    """Insert the exact canonical section using Commitizen's incremental merge."""
    metadata = Markdown(read_cfg("pyproject.toml")).get_metadata_from_file(
        StringIO(existing)
    )
    return "".join(
        changelog.incremental_build(
            section, existing.splitlines(keepends=True), metadata
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--working-directory", default=".")
    parser.add_argument("--release-paths", required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument(
        "--output", required=True, help="New UTF-8 Markdown file, outside the checkout"
    )
    args = parser.parse_args()
    try:
        os.chdir(args.working_directory)
        if git_output("status", "--porcelain", "--untracked-files=no"):
            raise CheckError(
                "Tracked files must be clean before preparing release notes."
            )
        output = Path(args.output).resolve()
        if output.is_relative_to(
            Path(git_output("rev-parse", "--show-toplevel")).resolve()
        ):
            raise CheckError(
                "Write the canonical release section outside the checkout."
            )
        paths = [
            line.strip() for line in args.release_paths.splitlines() if line.strip()
        ]
        if not paths:
            raise CheckError("Configure release_paths to cover packaged content.")
        release = preview(paths)
        section = release_section(
            release, PullRequests(args.repository, os.environ.get("GH_TOKEN", ""))
        )
        with TemporaryDirectory(dir=output.parent) as temporary:
            staged = Path(temporary) / output.name
            staged.write_text(section, encoding="utf-8")
            os.link(staged, output)
    except CheckError as error:
        print(f"Release notes failed: {error}", file=sys.stderr)
        return 1
    except (CommitizenException, KeyError, ValueError, OSError):
        print(
            "Release notes failed: invalid metadata, tooling, or output file.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
