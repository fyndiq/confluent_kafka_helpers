"""Canonical notes from temporary Git histories and mocked GitHub responses."""

import json
import sys
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import release_notes as notes
import test_library_ci as fixtures
from library_ci import CheckError


def pull(number=42, merged=True, repository="fyndiq/test-library", sha=None):
    return {
        "number": number,
        "merged_at": "2026-11-02T12:00:00Z" if merged else None,
        "merge_commit_sha": sha,
        "base": {"repo": {"full_name": repository}},
    }


def response(data):
    return BytesIO(json.dumps(data).encode())


class PullRequestLookup(unittest.TestCase):
    def setUp(self):
        self.pulls = notes.PullRequests("fyndiq/test-library", "test-token")
        self.sha = "a" * 40

    def test_merged_association_is_cached(self):
        with patch.object(notes, "urlopen", return_value=response([pull()])) as api:
            self.assertEqual(self.pulls.link(self.sha), self.pulls.link(self.sha))
        api.assert_called_once()
        request = api.call_args.args[0]
        self.assertIn(f"/commits/{self.sha}/pulls?", request.full_url)
        self.assertEqual(api.call_args.kwargs["timeout"], 30)
        self.assertEqual(
            self.pulls.cache[self.sha],
            "[#42](https://github.com/fyndiq/test-library/pull/42)",
        )

    def test_empty_success_and_unmerged_or_foreign_prs_fall_back(self):
        for data in ([], [pull(merged=False)], [pull(repository="other/repo")]):
            with (
                self.subTest(data=data),
                patch.object(notes, "urlopen", return_value=response(data)) as api,
            ):
                self.pulls.cache.clear()
                link = self.pulls.link(self.sha)
                self.assertIn(f"/commit/{self.sha}", link)
                self.assertEqual(self.pulls.link(self.sha), link)
                api.assert_called_once()

    def test_exact_merge_sha_wins_over_other_merged_association(self):
        with patch.object(
            notes, "urlopen", return_value=response([pull(7), pull(8, sha=self.sha)])
        ):
            self.assertIn("/pull/8)", self.pulls.link(self.sha))

    def test_ambiguous_merged_associations_fail(self):
        with (
            patch.object(notes, "urlopen", return_value=response([pull(7), pull(8)])),
            self.assertRaisesRegex(CheckError, "Multiple merged"),
        ):
            self.pulls.link(self.sha)

    def test_pagination_selects_merged_pr_after_full_page(self):
        with patch.object(
            notes,
            "urlopen",
            side_effect=[response([pull(merged=False)] * 100), response([pull()])],
        ) as api:
            self.assertIn("/pull/42)", self.pulls.link(self.sha))
        self.assertIn("page=2", api.call_args.args[0].full_url)

    def test_api_and_malformed_responses_fail_without_diagnostics(self):
        errors = [
            URLError("private diagnostic"),
            HTTPError("https://api.github.com", 403, "private diagnostic", {}, None),
        ]
        for error in errors:
            with (
                self.subTest(error=type(error)),
                patch.object(notes, "urlopen", side_effect=error),
                self.assertRaisesRegex(CheckError, "lookup failed") as caught,
            ):
                self.pulls.link(self.sha)
            self.assertNotIn("private", str(caught.exception))
        for data in ({"message": "private diagnostic"}, [{}], [pull(number="bad")]):
            with (
                self.subTest(data=data),
                patch.object(notes, "urlopen", return_value=response(data)),
                self.assertRaisesRegex(CheckError, "lookup failed"),
            ):
                self.pulls.link(self.sha)
        self.assertEqual(self.pulls.cache, {})


class ReleaseNotes(unittest.TestCase):
    setUp = fixtures.LibraryCI.setUp
    git = fixtures.LibraryCI.git
    configure = fixtures.LibraryCI.configure
    commit = fixtures.LibraryCI.commit
    preview = fixtures.LibraryCI.preview
    unchanged = fixtures.LibraryCI.unchanged

    def section(self, associations=None):
        associations = associations or {}

        def api(request, **kwargs):
            sha = request.full_url.split("/commits/")[1].split("/")[0]
            return response(associations.get(sha, []))

        with patch.object(notes, "urlopen", side_effect=api):
            return notes.release_section(
                self.preview(), notes.PullRequests("fyndiq/test-library", "test-token")
            )

    def test_rebase_commits_distinct_changes_and_identical_entries(self):
        first = self.commit("fix(api): repair retry", "package.py")
        second = self.commit("fix(api): repair retry", "package.py")
        third = self.commit("feat: add capability", "package.py")
        with self.unchanged():
            section = self.section({sha: [pull()] for sha in (first, second, third)})
        self.assertTrue(section.startswith("## v1.3.0 ("))
        self.assertEqual(section.count("repair retry"), 1)
        self.assertEqual(section.count("/pull/42)"), 2)
        self.assertIn("- **api**: repair retry ([#42]", section)
        self.assertLess(section.index("### Feat"), section.index("### Fix"))

    def test_same_description_in_distinct_prs_or_scopes_is_preserved(self):
        first = self.commit("fix(api): repair", "package.py")
        second = self.commit("fix(api): repair", "package.py")
        third = self.commit("fix(client): repair", "package.py")
        section = self.section({first: [pull(1)], second: [pull(2)], third: [pull(1)]})
        self.assertEqual(section.count("repair"), 3)

    def test_custom_group_mapping_deduplicates_final_rendered_entries(self):
        with Path("pyproject.toml").open("a") as stream:
            stream.write('change_type_map = {feat = "Changes", fix = "Changes"}\n')
        first = self.commit("fix(api): repair", "package.py")
        second = self.commit("feat(api): repair", "package.py")
        third = self.commit("fix(client): repair", "package.py")
        fourth = self.commit("feat(api): repair", "package.py")
        section = self.section(
            {first: [pull(1)], second: [pull(1)], third: [pull(1)], fourth: [pull(2)]}
        )
        self.assertIn("### Changes", section)
        self.assertEqual(section.count("repair"), 3)
        self.assertEqual(section.count("- **api**: repair ([#1]"), 1)
        self.assertIn("- **api**: repair ([#2]", section)
        self.assertIn("- **client**: repair ([#1]", section)

    def test_same_rendered_entry_in_distinct_groups_is_preserved(self):
        first = self.commit("fix(api): repair", "package.py")
        second = self.commit("feat(api): repair", "package.py")
        section = self.section({first: [pull()], second: [pull()]})
        self.assertIn("### Feat", section)
        self.assertIn("### Fix", section)
        self.assertEqual(section.count("- **api**: repair ([#42]"), 2)

    def test_missing_association_uses_sha_not_prose(self):
        sha = self.commit("fix: repair (#999)", "package.py")
        section = self.section()
        self.assertIn(f"/commit/{sha}", section)
        self.assertNotIn("/pull/999", section)

    def test_breaking_subject_and_multiline_body_are_preserved(self):
        sha = self.commit(
            "feat!: replace API\n\nBREAKING CHANGE: callers must adapt\nsecond line",
            "package.py",
        )
        with patch.object(notes, "urlopen", return_value=response([pull()])) as api:
            section = notes.release_section(
                self.preview(), notes.PullRequests("fyndiq/test-library", "test-token")
            )
        api.assert_called_once()
        self.assertIn("## v2.0.0", section)
        self.assertIn("### BREAKING CHANGE", section)
        self.assertIn("callers must adapt\nsecond line ([#42]", section)
        self.assertIn("**BREAKING CHANGE**: replace API", section)
        self.assertIn(sha, api.call_args.args[0].full_url)

    def test_pre_one_policy_and_intermediate_tags(self):
        for zero, version in ((True, "0.3.0"), (False, "1.0.0")):
            with self.subTest(zero=zero):
                self.git("reset", "--hard", self.base)
                self.configure("0.2.0", zero=zero)
                self.commit("chore: establish baseline")
                self.git("tag", "-f", "v0.2.0")
                self.base = self.git("rev-parse", "HEAD")
                self.commit("feat!: replace API", "package.py")
                self.git("tag", "-f", "v1.0.0a0")
                self.commit("fix: final fix", "package.py")
                section = self.section()
                self.assertIn(f"## v{version} (", section)
                self.assertEqual(section.count("\n## "), 0)
                self.assertIn("final fix", section)
                self.assertNotIn("v1.0.0a0", section)

    def test_breaking_subject_for_non_changelog_type_stays_visible(self):
        self.commit("build!: replace backend", "package.py")
        section = self.section()
        self.assertIn("## v2.0.0", section)
        self.assertIn("**BREAKING CHANGE**: replace backend", section)

    def test_historical_merges_and_changelog_are_excluded(self):
        self.git("checkout", "-b", "old-branch")
        self.commit("fix: historical branch", "package.py")
        self.git("checkout", "master")
        self.git("merge", "--no-ff", "old-branch", "-m", "Merge historical branch")
        self.base = self.git("rev-parse", "HEAD")
        self.commit("fix: new fix", "package.py")
        Path("CHANGELOG.md").write_text("## v1.2.3\n\nHistorical content\n")
        section = self.section()
        self.assertIn("historical branch", section)
        self.assertIn("new fix", section)
        self.assertNotIn("Merge historical", section)
        self.assertNotIn("Historical content", section)
        updated = notes.updated_changelog(section, Path("CHANGELOG.md").read_text())
        self.assertTrue(updated.startswith(section + "\n## v1.2.3"))
        self.assertIn("Historical content", updated)

    def test_changelog_header_and_history_survive_canonical_insertion(self):
        self.commit("fix: repair", "package.py")
        section = self.section()
        previous = "# Changelog\n\n## v1.2.3\n\n- old entry\n"
        updated = notes.updated_changelog(section, previous)
        self.assertEqual(
            updated, "# Changelog\n\n" + section + "\n## v1.2.3\n\n- old entry\n"
        )
        next_section = section.replace("v1.2.4", "v1.2.5")
        self.assertEqual(
            notes.updated_changelog(next_section, updated),
            "# Changelog\n\n"
            + next_section
            + "\n"
            + updated.removeprefix("# Changelog\n\n"),
        )

    def test_caller_template_cannot_remove_links(self):
        self.commit("fix: repair", "package.py")
        Path("CHANGELOG.md.j2").write_text("wrong template")
        self.assertIn("/commit/", self.section())

    def test_no_increment_has_no_notes_or_network_call(self):
        self.commit("docs: explain usage")
        with (
            patch.object(notes, "urlopen") as api,
            self.assertRaisesRegex(CheckError, "No release-eligible"),
        ):
            notes.release_section(
                self.preview(), notes.PullRequests("fyndiq/test-library", "test-token")
            )
        api.assert_not_called()

    def test_cli_api_failure_writes_no_output(self):
        self.commit("fix: repair", "package.py")
        with (
            tempfile.TemporaryDirectory() as output_dir,
            patch.object(
                sys,
                "argv",
                [
                    "release_notes.py",
                    "--release-paths",
                    "package.py",
                    "--repository",
                    "fyndiq/test-library",
                    "--output",
                    str(Path(output_dir) / "release-notes.md"),
                ],
            ),
            patch.dict("os.environ", {"GH_TOKEN": "test-token"}),
            patch.object(
                notes, "urlopen", side_effect=URLError("private diagnostic")
            ) as api,
            patch("sys.stderr"),
        ):
            self.assertEqual(notes.main(), 1)
            self.assertFalse((Path(output_dir) / "release-notes.md").exists())
        api.assert_called_once()

    def test_cli_writes_canonical_file_and_rejects_replacement_or_dirty_metadata(self):
        self.commit("fix: repair", "package.py")
        with tempfile.TemporaryDirectory() as output_dir:
            output = Path(output_dir) / "release-notes.md"
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "release_notes.py",
                        "--release-paths",
                        "package.py",
                        "--repository",
                        "fyndiq/test-library",
                        "--output",
                        str(output),
                    ],
                ),
                patch.dict("os.environ", {"GH_TOKEN": "test-token"}),
                patch.object(
                    notes, "urlopen", side_effect=lambda *a, **k: response([])
                ),
                patch("sys.stderr"),
                self.unchanged(),
            ):
                self.assertEqual(notes.main(), 0)
                section = output.read_text()
                self.assertEqual(section, self.section())
                original_bytes = output.read_bytes()
                self.assertEqual(notes.main(), 1)
                self.assertEqual(output.read_bytes(), original_bytes)
                self.assertEqual(list(Path(output_dir).iterdir()), [output])
                output.unlink()
                Path("pyproject.toml").write_text("uncommitted metadata")
                with patch.object(notes, "urlopen") as api:
                    self.assertEqual(notes.main(), 1)
                    api.assert_not_called()
                self.assertFalse(output.exists())
                self.git("checkout", "--", "pyproject.toml")

    def test_cli_output_failures_leave_no_partial_file_and_allow_retry(self):
        self.commit("fix: repair", "package.py")
        write_text = Path.write_text

        def partial_write(path, section, **kwargs):
            write_text(path, section[:10], **kwargs)
            raise OSError("private write diagnostic")

        for operation in ("write", "publish"):
            with (
                self.subTest(operation=operation),
                tempfile.TemporaryDirectory() as output_dir,
                patch.object(
                    sys,
                    "argv",
                    [
                        "release_notes.py",
                        "--release-paths",
                        "package.py",
                        "--repository",
                        "fyndiq/test-library",
                        "--output",
                        str(Path(output_dir) / "release-notes.md"),
                    ],
                ),
                patch.dict("os.environ", {"GH_TOKEN": "test-token"}),
                patch.object(
                    notes, "urlopen", side_effect=lambda *a, **k: response([])
                ),
                patch("sys.stderr") as stderr,
            ):
                output = Path(output_dir) / "release-notes.md"
                failure = (
                    patch.object(
                        Path, "write_text", autospec=True, side_effect=partial_write
                    )
                    if operation == "write"
                    else patch.object(
                        notes.os,
                        "link",
                        side_effect=OSError("private publication diagnostic"),
                    )
                )
                with failure:
                    self.assertEqual(notes.main(), 1)
                self.assertEqual(list(Path(output_dir).iterdir()), [])
                self.assertNotIn("private", str(stderr.write.call_args_list))
                self.assertEqual(notes.main(), 0)
                self.assertEqual(output.read_text(), self.section())
                self.assertEqual(list(Path(output_dir).iterdir()), [output])


if __name__ == "__main__":
    unittest.main()
