"""Exercise the shared contract against real temporary Git histories."""

import os
import subprocess
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import library_ci as ci


class LibraryCI(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.previous = Path.cwd()
        os.chdir(self.temp.name)
        self.addCleanup(os.chdir, self.previous)
        self.git("init", "-b", "master")
        self.git("config", "user.email", "ci@example.invalid")
        self.git("config", "user.name", "CI Test")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        self.git("config", "core.hooksPath", "/dev/null")
        self.configure()
        Path("package.py").write_text("initial\n")
        self.commit("historical nonconventional commit")
        self.git("tag", "v1.2.3")
        self.base = self.git("rev-parse", "HEAD")

    def git(self, *args):
        result = subprocess.run(
            ["git", *args], capture_output=True, text=True, check=False
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def configure(
        self, version="1.2.3", zero=False, provider="pep621", tag_format="v$version"
    ):
        Path("pyproject.toml").write_text(
            f'[project]\nname = "test-library"\nversion = "{version}"\n'
            '[tool.commitizen]\nname = "cz_conventional_commits"\n'
            f'version_provider = "{provider}"\ntag_format = "{tag_format}"\n'
            f'major_version_zero = {str(zero).lower()}\nversion_scheme = "pep440"\n'
            'pre_bump_hooks = ["touch should-not-exist"]\n'
        )

    def commit(self, message, path="notes.md"):
        with open(path, "a") as stream:
            stream.write(message + "\n")
        self.git("add", ".")
        self.git("commit", "-m", message)
        return self.git("rev-parse", "HEAD")

    def preview(self):
        return ci.preview(["package.py", "pyproject.toml"], self.base)

    @contextmanager
    def unchanged(self):
        before = self.git("status", "--porcelain"), self.git("show-ref")
        yield
        self.assertEqual(
            before, (self.git("status", "--porcelain"), self.git("show-ref"))
        )
        self.assertFalse(Path("should-not-exist").exists())

    def test_all_introduced_commits_are_checked(self):
        invalid = self.commit("invalid middle commit")
        self.commit("fix: valid latest", "package.py")
        with self.assertRaisesRegex(ci.CheckError, invalid[:12]):
            self.preview()

    def test_multiple_valid_commits_and_read_only_preview(self):
        self.commit("fix: repair", "package.py")
        self.commit("feat: add capability", "package.py")
        with self.unchanged():
            self.assertEqual(self.preview()["next-version"], "1.3.0")

    def test_complete_breaking_body_drives_major(self):
        self.commit(
            "fix: alter API\n\nBREAKING CHANGE: callers must adapt", "package.py"
        )
        self.assertEqual(self.preview()["next-version"], "2.0.0")

    def test_breaking_subject(self):
        self.commit("feat!: alter API", "package.py")
        self.assertEqual(self.preview()["next-version"], "2.0.0")

    def test_invalid_body_separator_fails(self):
        self.commit("fix: repair\nbody without blank separator", "package.py")
        with self.assertRaisesRegex(ci.CheckError, "Invalid Conventional"):
            self.preview()

    def test_raw_commit_records_preserve_multiline_messages(self):
        messages = [
            "fix: preserve text\n\nBody with  trailing spaces  \n\nFooter: value\n",
            "docs: another record\n\nBody with \x1e and \x1f separators\n",
        ]
        revisions = []
        for message in messages:
            self.git("commit", "--allow-empty", "--cleanup=verbatim", "-m", message)
            revisions.append(self.git("rev-parse", "HEAD"))
        records = ci.commits(self.base, "HEAD")
        self.assertEqual([record.rev for record in records], revisions)
        self.assertEqual([record.message for record in records], messages)

    def test_explicit_pyproject_config_ignores_shadow_file(self):
        Path(".cz.toml").write_text(
            '[tool.commitizen]\nname = "cz_conventional_commits"\n'
            'version_provider = "pep621"\nversion_scheme = "invalid-scheme"\n'
        )
        self.commit("fix: repair", "package.py")
        with self.unchanged():
            self.assertEqual(self.preview()["next-version"], "1.2.4")

    def test_supported_tag_formats_match_commitizen_preview(self):
        for tag_format, baseline in [
            ("v$major.$minor.$patch", "v1.2.3"),
            ("release-${version}", "release-1.2.3"),
            ("$version", "1.2.3"),
        ]:
            with self.subTest(tag_format=tag_format):
                self.git("reset", "--hard", self.base)
                self.configure(tag_format=tag_format)
                self.commit("chore: configure tags")
                self.git("tag", "-f", baseline)
                self.base = self.git("rev-parse", "HEAD")
                self.commit("fix: repair", "package.py")
                result = self.preview()
                self.assertEqual(result["baseline"], baseline)
                self.assertEqual(result["next-version"], "1.2.4")

    def test_legacy_tag_format_resolves_stable_baseline(self):
        self.configure(tag_format="release-$version")
        with Path("pyproject.toml").open("a") as stream:
            stream.write('legacy_tag_formats = ["v$version"]\n')
        self.commit("fix: repair", "package.py")
        result = self.preview()
        self.assertEqual(result["baseline"], "v1.2.3")
        self.assertEqual(result["next-version"], "1.2.4")

    def test_pre_one_policy_is_preserved(self):
        for zero, expected in [(True, "0.3.0"), (False, "1.0.0")]:
            with self.subTest(major_version_zero=zero):
                self.git("reset", "--hard", self.base)
                self.configure("0.2.0", zero=zero)
                self.commit("chore: establish pre-one baseline")
                self.git("tag", "-f", "v0.2.0")
                self.base = self.git("rev-parse", "HEAD")
                self.commit(
                    "feat: replace API\n\nBREAKING CHANGE: new API", "package.py"
                )
                self.assertEqual(self.preview()["next-version"], expected)

    def test_known_no_increment(self):
        self.commit("docs: explain usage")
        self.assertEqual(self.preview()["next-version"], "")

    def test_no_commits_at_stable_baseline(self):
        self.assertEqual(ci.preview(["package.py"])["next-version"], "")

    def test_config_failure_at_stable_head_is_not_skipped(self):
        text = (
            Path("pyproject.toml")
            .read_text()
            .replace('version_scheme = "pep440"', 'version_scheme = "invalid-scheme"')
        )
        Path("pyproject.toml").write_text(text)
        self.git("add", ".")
        self.git("commit", "--amend", "--no-edit")
        self.git("tag", "-f", "v1.2.3")
        with self.assertRaisesRegex(ci.CheckError, "Commitizen preview failed"):
            ci.preview(["package.py"])

    def test_release_paths_are_repository_relative_from_subdirectory(self):
        Path("subproject").mkdir()
        self.git("mv", "pyproject.toml", "subproject/pyproject.toml")
        self.git("mv", "package.py", "subproject/package.py")
        self.commit("chore: establish subproject baseline")
        self.git("tag", "-f", "v1.2.3")
        self.base = self.git("rev-parse", "HEAD")
        self.commit("chore: change package", "subproject/package.py")
        os.chdir("subproject")
        with self.assertRaisesRegex(ci.CheckError, "Packaged content changed"):
            ci.preview(["subproject/package.py"], self.base)

    def test_packaged_content_without_release_commit_fails(self):
        self.commit("chore: alter packaged asset", "package.py")
        with self.assertRaisesRegex(ci.CheckError, "Packaged content changed"):
            self.preview()

    def test_pr_cannot_borrow_earlier_eligible_commit(self):
        self.commit("feat: earlier unreleased change", "package.py")
        self.base = self.git("rev-parse", "HEAD")
        self.commit("chore: change source", "package.py")
        with self.assertRaisesRegex(ci.CheckError, "Packaged content changed"):
            self.preview()

    def test_deleted_packaged_asset_is_detected(self):
        Path("package.py").unlink()
        self.commit("docs: remove asset")
        with self.assertRaisesRegex(ci.CheckError, "Packaged content changed"):
            self.preview()

    def test_historical_merge_commits_remain_readable(self):
        self.git("checkout", "-b", "old-branch")
        self.commit("fix: historical branch", "package.py")
        self.git("checkout", "master")
        self.git("merge", "--no-ff", "old-branch", "-m", "Merge historical branch")
        self.base = self.git("rev-parse", "HEAD")
        self.commit("docs: explain history")
        self.assertEqual(self.preview()["next-version"], "1.2.4")

    def test_new_merge_commit_does_not_bypass_validation(self):
        self.git("checkout", "-b", "new-branch")
        self.commit("fix: branch", "package.py")
        self.git("checkout", "master")
        self.git("merge", "--no-ff", "new-branch", "-m", "Merge new branch")
        with self.assertRaisesRegex(ci.CheckError, "Invalid Conventional"):
            self.preview()

    def test_missing_baseline_is_an_error(self):
        self.git("tag", "-d", "v1.2.3")
        self.commit("fix: repair", "package.py")
        with self.assertRaisesRegex(ci.CheckError, "Fetch complete history"):
            self.preview()

    def test_unreachable_baseline_is_an_error(self):
        self.git("checkout", "-b", "other")
        self.commit("fix: unrelated baseline")
        self.git("tag", "-f", "v1.2.3")
        self.git("checkout", "master")
        self.commit("fix: repair", "package.py")
        with self.assertRaisesRegex(ci.CheckError, "must be an ancestor"):
            self.preview()

    def test_missing_pr_history_is_an_error(self):
        self.commit("fix: repair", "package.py")
        with self.assertRaisesRegex(ci.CheckError, "Fetch complete history"):
            ci.preview(["package.py"], "f" * 40)

    def test_shallow_history_is_an_error(self):
        clone = Path(self.temp.name) / "shallow"
        self.git("clone", "--depth", "1", Path.cwd().as_uri(), str(clone))
        os.chdir(clone)
        with self.assertRaisesRegex(ci.CheckError, "Shallow history"):
            ci.preview(["package.py"])

    def test_prerelease_tag_does_not_replace_stable_baseline(self):
        self.commit("feat: upcoming feature", "package.py")
        self.git("tag", "v2.0.0a0")
        self.assertEqual(self.preview()["next-version"], "1.3.0")

    def test_uv_provider_preview(self):
        self.configure(provider="uv")
        self.commit("fix: repair", "package.py")
        self.assertEqual(self.preview()["next-version"], "1.2.4")

    def test_unexpected_preview_failure_is_not_skipped(self):
        self.commit("fix: repair", "package.py")
        original = ci.run

        def failed_preview(*args):
            if "commitizen" in args:
                return subprocess.CompletedProcess(
                    args, 99, "sensitive output", "private diagnostic"
                )
            return original(*args)

        with (
            patch.object(ci, "run", side_effect=failed_preview),
            self.assertRaisesRegex(ci.CheckError, "exit 99") as error,
        ):
            self.preview()
        self.assertNotIn("sensitive", str(error.exception))

    def test_no_commits_exit_with_nonempty_range_is_failure(self):
        self.commit("docs: no release")
        original = ci.run

        def unexpected_empty(*args):
            if "commitizen" in args:
                return subprocess.CompletedProcess(args, 3, "", "")
            return original(*args)

        with (
            patch.object(ci, "run", side_effect=unexpected_empty),
            self.assertRaisesRegex(ci.CheckError, "exit 3"),
        ):
            self.preview()

    def test_build_failure_does_not_expose_diagnostics(self):
        with (
            patch.object(
                ci,
                "run",
                return_value=subprocess.CompletedProcess([], 1, "secret", "secret"),
            ),
            self.assertRaisesRegex(ci.CheckError, "Distribution build failed") as error,
        ):
            ci.build()
        self.assertNotIn("secret", str(error.exception))


if __name__ == "__main__":
    unittest.main()
