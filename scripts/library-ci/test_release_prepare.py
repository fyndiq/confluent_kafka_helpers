"""Preparation snapshots and validation before source builds."""

import os
import unittest
from pathlib import Path
from unittest.mock import patch

import library_release as release
from release_test_support import PreparationFixture


class PreparationTests(PreparationFixture, unittest.TestCase):
    def test_partial_prepare_rerun_checks_durable_history_before_build(self):
        from release_test_support import ArtifactAPI

        for result in ("success", "failure", "cancelled", None):
            store = ArtifactAPI()
            if result:
                store.receipt(preparation=result)
            with (
                self.subTest(preparation=result),
                patch.dict(
                    os.environ,
                    {
                        "GITHUB_RUN_ID": "42",
                        "GITHUB_RUN_ATTEMPT": "2",
                        "DISPATCH_TOKEN": "test",
                    },
                ),
                patch.object(release.GitHub, "request", side_effect=store.request),
                patch.object(release, "checked") as build,
            ):
                with self.assertRaisesRegex(release.CheckError, "without rebuilding"):
                    release.prepare(
                        ["package.py"],
                        self.candidate,
                        self.github,
                        inspected_attempt="1",
                    )
                build.assert_not_called()

    def test_partial_prepare_after_checks_failure_and_later_whole_retry_remain_safe(
        self,
    ):
        from release_recovery import RecoveryStore, preparation_receipt
        from release_test_support import ArtifactAPI

        store = ArtifactAPI().receipt()
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_RUN_ID": "42",
                    "GITHUB_RUN_ATTEMPT": "2",
                    "DISPATCH_TOKEN": "test",
                    "RUNNER_TEMP": str(self.root),
                },
            ),
            patch.object(release.GitHub, "request", side_effect=store.request),
        ):
            release.prepare(
                ["package.py"], self.candidate, self.github, inspected_attempt="1"
            )
            self.assertFalse(self.candidate.exists())
            preparation_receipt(
                self.github.repository, "success", "success", mode="stable"
            )
            store.add(
                "library-preparation-2",
                {
                    "preparation.json": (
                        self.root / "library-preparation/preparation.json"
                    ).read_bytes()
                },
            )
            RecoveryStore(self.github.repository).require_unprepared("42", "3")

    def test_real_commitizen_updates_only_package_lock_and_retains_canonical_notes(
        self,
    ):
        import tomllib

        Path("package.py").write_text("value = 2\n")
        self.commit("fix: repair package")
        source = self.git("rev-parse", "HEAD")
        self.real_checked = release.checked
        with (
            patch.object(release, "checked", side_effect=self.build),
            patch.object(
                release.PullRequests,
                "link",
                return_value="[#1](https://github.com/fyndiq/example/pull/1)",
            ),
            patch.dict(
                os.environ,
                {
                    "GITHUB_RUN_ID": "42",
                    "GITHUB_RUN_ATTEMPT": "3",
                    "GITHUB_OUTPUT": str(self.root / "outputs"),
                },
            ),
        ):
            release.prepare(
                ["package.py", "pyproject.toml", "uv.lock"],
                self.candidate,
                self.github,
                inspected_attempt="3",
            )
        self.assertIn("candidate-attempt=3\n", (self.root / "outputs").read_text())
        manifest = release.load(self.candidate, self.github.repository)
        self.assertEqual(manifest.source_sha, source)
        self.assertEqual(manifest.release_sha, self.git("rev-parse", "HEAD"))
        self.assertEqual(self.git("rev-parse", "HEAD^"), source)
        self.assertEqual(
            Path("CHANGELOG.md").read_bytes(),
            (self.candidate / "notes.md").read_bytes(),
        )
        self.assertEqual(
            [
                p["version"]
                for p in tomllib.loads(Path("uv.lock").read_text())["package"]
            ],
            ["1.0.1", "1.0.0"],
        )
        self.assertEqual(
            tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"],
            "1.0.1",
        )
        self.assertFalse(Path("forbidden-hook").exists())
        self.assertEqual(self.git("status", "--porcelain", "--untracked-files=no"), "")
        self.git("bundle", "verify", str(self.candidate / "release.bundle"))

    def test_prerelease_snapshot_preserves_changelog_and_changes_only_version_files(
        self,
    ):
        Path("CHANGELOG.md").write_text("# Existing changelog\n")
        Path("package.py").write_text("value = 2\n")
        self.commit("feat: next feature")
        source = self.git("rev-parse", "HEAD")
        self.real_checked = release.checked
        with (
            patch.object(release, "checked", side_effect=self.build),
            patch.object(release.PullRequests, "link", return_value="feature"),
            patch.dict(os.environ, {"GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "1"}),
        ):
            release.prepare(
                ["package.py"],
                self.candidate,
                self.github,
                "2.0.0rc1",
                source,
                mode="prerelease",
            )
        candidate = release.load(self.candidate, self.github.repository)
        self.assertTrue(candidate.prerelease)
        self.assertEqual(candidate.version, "2.0.0rc1")
        self.assertEqual(self.git("rev-parse", "refs/heads/master"), source)
        self.assertEqual(Path("CHANGELOG.md").read_text(), "# Existing changelog\n")
        self.assertEqual(
            set(self.git("diff", "--name-only", source, "HEAD").splitlines()),
            {"pyproject.toml", "uv.lock"},
        )
        self.assertEqual(self.git("rev-parse", "HEAD^"), source)

    def test_no_increment_creates_no_candidate(self):
        release.prepare(["package.py"], self.candidate, self.github)
        self.assertFalse(self.candidate.exists())
        self.assertEqual(self.git("rev-parse", "HEAD"), self.baseline)

    def test_invalid_full_release_range_fails_before_build(self):
        Path("package.py").write_text("value = 2\n")
        self.commit("invalid commit")
        Path("package.py").write_text("value = 3\n")
        self.commit("fix: valid final commit")
        with (
            patch.object(release, "checked") as checked,
            self.assertRaisesRegex(release.CheckError, "Invalid Conventional"),
        ):
            release.prepare(["package.py"], self.candidate, self.github)
        checked.assert_not_called()
        self.assertFalse(self.candidate.exists())

    def test_historical_merge_preserves_branch_increment_and_breaking_body(self):
        for message, version in (
            ("fix: repair package", "1.0.1"),
            ("fix: replace API\n\nBREAKING CHANGE: callers must adapt", "2.0.0"),
        ):
            with self.subTest(message=message):
                self.git("checkout", "master")
                self.git("reset", "--hard", self.baseline)
                self.git("checkout", "-B", "historical-branch")
                Path("package.py").write_text("value = 2\n")
                self.commit(message)
                self.git("checkout", "master")
                self.git(
                    "merge", "--no-ff", "historical-branch", "-m", "Integrated branch"
                )
                # Stop before committing the prepared version or building artifacts.
                with (
                    patch.object(
                        release,
                        "checked",
                        side_effect=RuntimeError("preparation reached"),
                    ),
                    patch.object(release.PullRequests, "link", return_value="branch"),
                    patch.object(release, "outputs") as outputs,
                    self.assertRaisesRegex(RuntimeError, "preparation reached"),
                ):
                    release.prepare(["package.py"], self.root / version, self.github)
                self.assertEqual(
                    outputs.call_args_list[0].kwargs["next_version"], version
                )

    def test_invalid_ordinary_commits_are_rejected_in_merged_history(self):
        for message in ("invalid branch commit", "Merge misleading ordinary commit"):
            with self.subTest(message=message):
                self.git("reset", "--hard", self.baseline)
                self.git("checkout", "-B", "historical-branch")
                Path("package.py").write_text("value = 2\n")
                self.commit(message)
                invalid = self.git("rev-parse", "HEAD")
                self.git("checkout", "master")
                self.git(
                    "merge", "--no-ff", "historical-branch", "-m", "Integrated branch"
                )
                Path("package.py").write_text("value = 3\n")
                self.commit("fix: valid final commit")
                with (
                    patch.object(release, "checked") as checked,
                    self.assertRaisesRegex(release.CheckError, invalid[:12]),
                ):
                    release.prepare(["package.py"], self.candidate, self.github)
                checked.assert_not_called()
                self.assertFalse(self.candidate.exists())

    def test_historical_merge_cannot_replace_a_release_eligible_commit(self):
        self.git("checkout", "-b", "historical-branch")
        Path("package.py").write_text("value = 2\n")
        self.commit("chore: alter package")
        self.git("checkout", "master")
        self.git("merge", "--no-ff", "historical-branch", "-m", "Integrated branch")
        with (
            patch.object(release, "checked") as checked,
            self.assertRaisesRegex(
                release.CheckError, "without a release-eligible commit"
            ),
        ):
            release.prepare(["package.py"], self.candidate, self.github)
        checked.assert_not_called()
        self.assertFalse(self.candidate.exists())
