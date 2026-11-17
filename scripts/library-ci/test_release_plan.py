"""Trusted inspection, allocation and recovery choices."""

import copy
import json
import os
import unittest
from itertools import product
from pathlib import Path
from unittest.mock import MagicMock, patch

import library_release as release
import release_plan as planning
from release_test_support import (
    ArtifactAPI,
    LifecycleFixture,
    PreparationFixture,
    Registry,
    assert_outputs,
    registries,
)


class LifecycleTests(LifecycleFixture, unittest.TestCase):
    def inspect_retry(self, store):
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_RUN_ID": "42",
                    "GITHUB_RUN_ATTEMPT": "2",
                    "DISPATCH_TOKEN": "test",
                },
            ),
            patch.object(release.GitHub, "request", side_effect=store.request),
            patch.object(
                self.github,
                "request",
                side_effect=AssertionError("source must not be resolved"),
            ),
            patch.object(release, "outputs"),
        ):
            return release.inspect(
                self.github, "", "master", "feature", "2.0.0", "alpha"
            )

    def allocated_store(self):
        return ArtifactAPI().add(
            "library-allocation",
            {
                "allocation.json": json.dumps(
                    {
                        "repository": self.github.repository,
                        "run_id": "42",
                        "source_sha": self.source,
                        "version": "2.0.0a1",
                    }
                )
            },
        )

    def test_missing_candidate_fails_after_preparation_even_if_cleanup_failed_or_job_renamed(
        self,
    ):
        for result, inspection in product(
            ("success", "failure", "cancelled"), repeat=2
        ):
            with (
                self.subTest(preparation=result, inspection=inspection),
                self.assertRaisesRegex(release.CheckError, "without rebuilding"),
            ):
                self.inspect_retry(
                    self.allocated_store().receipt(
                        preparation=result, inspection=inspection
                    )
                )

    def test_missing_preparation_receipt_fails_closed(self):
        with self.assertRaisesRegex(release.CheckError, "without rebuilding"):
            self.inspect_retry(self.allocated_store())

    def test_skipped_preparation_preserves_allocation_after_checks_failure(self):
        for inspection in ("success", "failure", "cancelled"):
            with self.subTest(inspection=inspection):
                result = self.inspect_retry(
                    self.allocated_store().receipt(inspection=inspection)
                )
                self.assertEqual(
                    result,
                    planning.Inspection(
                        "prepare",
                        "prerelease",
                        self.source,
                        "2.0.0a1",
                        allocation_retained=True,
                    ),
                )

    def test_missing_allocation_after_inspection_refuses_resolving_source_again(self):
        for inspection in ("success", "failure", "cancelled"):
            with (
                self.subTest(inspection=inspection),
                self.assertRaisesRegex(release.CheckError, "without rebuilding"),
            ):
                self.inspect_retry(ArtifactAPI().receipt(inspection=inspection))

    def test_no_increment_receipt_permits_stable_retry_without_candidate(self):
        from release_recovery import RecoveryStore

        store = ArtifactAPI().receipt(preparation="no-increment")
        with (
            patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
            patch.object(release.GitHub, "request", side_effect=store.request),
        ):
            RecoveryStore(self.github.repository).require_unprepared("42", "2")

    def test_no_increment_receipt_requires_successful_or_skipped_inspection(self):
        from release_recovery import RecoveryStore

        for inspection in ("failure", "cancelled"):
            store = ArtifactAPI().receipt(
                preparation="no-increment", inspection=inspection
            )
            with (
                self.subTest(inspection=inspection),
                patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
                patch.object(release.GitHub, "request", side_effect=store.request),
                self.assertRaisesRegex(release.CheckError, "without rebuilding"),
            ):
                RecoveryStore(self.github.repository).require_unprepared("42", "2")

    def test_later_skipped_receipt_cannot_hide_earlier_preparation(self):
        from release_recovery import RecoveryStore

        store = ArtifactAPI().receipt(preparation="success").receipt(attempt="2")
        with (
            patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
            patch.object(release.GitHub, "request", side_effect=store.request),
            self.assertRaisesRegex(release.CheckError, "without rebuilding"),
        ):
            RecoveryStore(self.github.repository).require_unprepared("42", "3")

    def test_request_and_inspection_reject_invalid_state(self):
        with self.assertRaises(release.CheckError):
            planning.ReleaseRequest("v1.0.1", "feature", "2.0.0", "alpha")
        for outcome, mode, version, tag, attempt in [
            ("reuse", "stable", "1.0.1", "", ""),
            ("resume", "stable", "", "", ""),
            ("prepare", "prerelease", "1.0.1", "", ""),
        ]:
            with self.subTest(outcome=outcome), self.assertRaises(release.CheckError):
                planning.Inspection(outcome, mode, self.source, version, tag, attempt)

    def test_pending_prerelease_preserves_requested_followup_intent(self):
        self.prerelease_candidate()
        self.github.fail_upload = "manifest.json"
        with self.assertRaises(release.CheckError):
            self.publish()
        for request, expected in [
            (planning.ReleaseRequest(), True),
            (planning.ReleaseRequest(resume_tag="v2.0.0a1"), False),
            (
                planning.ReleaseRequest(
                    source_ref="feature", target_version="2.0.0", phase="alpha"
                ),
                False,
            ),
        ]:
            with self.subTest(intent=request.intent):
                result = planning.plan(self.github, request)
                self.assertEqual(result.outcome, "resume")
                self.assertEqual(result.mode, "prerelease")
                self.assertEqual(result.followup_stable, expected)

    def test_prerelease_run_retry_reuses_and_validates_original_candidate(self):
        self.prerelease_candidate()
        self.advance()
        allocation = {
            "repository": self.github.repository,
            "run_id": "42",
            "source_sha": self.source,
            "version": self.manifest["version"],
        }
        for failure in (None, "hash", "transport", "allocation"):
            with self.subTest(failure=failure):
                manifest = copy.deepcopy(self.manifest)
                if failure == "transport":
                    manifest["transport"]["run_id"] = "43"
                if failure == "allocation":
                    manifest["source_sha"] = "0" * 40
                files = {
                    path.name: path.read_bytes() for path in self.directory.iterdir()
                }
                files["manifest.json"] = json.dumps(manifest).encode()
                if failure == "hash":
                    files["notes.md"] = b"corrupt"
                store = (
                    ArtifactAPI()
                    .add("library-candidate-1", files)
                    .add(
                        "library-allocation",
                        {"allocation.json": json.dumps(allocation)},
                    )
                )
                with (
                    patch.dict(
                        os.environ,
                        {
                            "GITHUB_RUN_ID": "42",
                            "GITHUB_RUN_ATTEMPT": "2",
                            "DISPATCH_TOKEN": "test",
                        },
                    ),
                    patch.object(
                        release.GitHub,
                        "request",
                        side_effect=store.request,
                    ),
                    patch.object(release, "outputs") as output,
                ):
                    if failure:
                        with self.assertRaises(release.CheckError):
                            release.inspect(
                                self.github, "", "master", "feature", "2.0.0", "alpha"
                            )
                        output.assert_not_called()
                    else:
                        release.inspect(
                            self.github, "", "master", "feature", "2.0.0", "alpha"
                        )
                        assert_outputs(
                            output,
                            resume_tag="",
                            source_sha=self.source,
                            next_version="2.0.0a1",
                            candidate_attempt="1",
                        )

    def test_publisher_retry_after_draft_creation_before_any_upload(self):
        self.github.fail_upload = "manifest.json"
        with self.assertRaises(release.CheckError):
            self.publish()
        self.assertEqual(self.github.data, {})
        store = ArtifactAPI().add(
            "library-candidate-1",
            {p.name: p.read_bytes() for p in self.directory.iterdir()},
        )
        self.github.fail_upload = ""
        self.github.records[0]["name"] = "Renamed by an operator"
        with patch.object(release, "outputs") as output:
            release.inspect(self.github, "", "master")
        assert_outputs(output, resume_tag="v1.0.1", source_sha=self.sha)
        with (
            patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
            patch.object(
                release.GitHub,
                "request",
                side_effect=store.request,
            ) as request,
        ):
            self.publish()
        self.assertEqual(
            request.call_args_list[0].args[1],
            "actions/runs/42/artifacts?per_page=100&page=1",
        )
        self.assertEqual(len(self.github.records), 1)
        self.assertFalse(self.github.records[0]["draft"])
        self.assertEqual(
            self.github.records[0]["body"], (self.directory / "notes.md").read_text()
        )

    def test_inspection_rejects_automation_tag_with_missing_release(self):
        from types import SimpleNamespace

        self.git(
            "tag", "-a", "v1.0.1", self.sha, "-m", release.MARKER + "\n" + "a" * 64
        )
        with (
            patch.object(
                planning,
                "project",
                return_value=(
                    {"project": {"name": "example", "version": "1.0.0"}},
                    self.manifest["index"],
                ),
            ),
            patch.object(
                planning,
                "read_cfg",
                return_value=SimpleNamespace(settings={"tag_format": "v$version"}),
            ),
            patch.object(
                planning.TagRules,
                "from_settings",
                return_value=SimpleNamespace(normalize_tag=lambda _: "v1.0.0"),
            ),
            patch.object(planning, "Registries", return_value=self.registries),
            patch.object(planning, "verify_baseline"),
            self.assertRaisesRegex(release.CheckError, "lost its retained release"),
        ):
            release.inspect(self.github, "", "master")

    def test_unnamed_draft_does_not_block_inspection_or_publication(self):
        from types import SimpleNamespace

        self.github.records.append(
            {"id": 99, "tag_name": "unrelated", "draft": True, "name": None}
        )
        with (
            patch.object(
                planning,
                "project",
                return_value=(
                    {"project": {"name": "example", "version": "1.0.0"}},
                    self.manifest["index"],
                ),
            ),
            patch.object(
                planning, "read_cfg", return_value=SimpleNamespace(settings={})
            ),
            patch.object(
                planning.TagRules,
                "from_settings",
                return_value=SimpleNamespace(normalize_tag=lambda _: "v1.0.0"),
            ),
            patch.object(planning, "Registries", return_value=self.registries),
            patch.object(release, "outputs") as output,
        ):
            release.inspect(self.github, "", "master")
        assert_outputs(output, resume_tag="", source_sha=self.sha)
        self.publish()
        self.assertTrue(self.github.records[0]["draft"])
        self.assertFalse(self.github.records[1]["draft"])


class PreparationTests(PreparationFixture, unittest.TestCase):
    def test_stable_retry_after_failed_inspection_and_skipped_preparation(self):
        for inspection in ("failure", "cancelled"):
            store = ArtifactAPI().receipt(inspection=inspection)
            with (
                self.subTest(inspection=inspection),
                patch.dict(
                    os.environ,
                    {
                        "GITHUB_RUN_ID": "42",
                        "GITHUB_RUN_ATTEMPT": "2",
                        "DISPATCH_TOKEN": "test",
                    },
                ),
                patch.object(release.GitHub, "request", side_effect=store.request),
                patch.object(planning, "Registries", return_value=registries()),
                patch.object(release, "outputs"),
            ):
                result = release.inspect(self.github, "", "master")
                self.assertEqual(
                    result, planning.Inspection("prepare", "stable", self.baseline)
                )

    def test_allocation_counts_registry_and_tags_separately_per_phase(self):
        self.git("tag", "v2.0.0a3")
        self.git("tag", "v2.0.0b2")
        registry = MagicMock()
        registry.versions.return_value = {
            release.Version("2.0.0a5"),
            release.Version("2.0.0rc4"),
        }
        metadata = release.tomllib.loads(Path("pyproject.toml").read_text())
        for phase, expected in [
            ("alpha", "2.0.0a6"),
            ("beta", "2.0.0b3"),
            ("rc", "2.0.0rc5"),
        ]:
            with self.subTest(phase=phase):
                self.assertEqual(
                    planning.allocate_prerelease(metadata, registry, "2.0.0", phase),
                    expected,
                )
        self.git("tag", "v2.0.0")
        with self.assertRaisesRegex(release.CheckError, "already exists"):
            planning.allocate_prerelease(metadata, registry, "2.0.0", "alpha")

    def test_inspection_keeps_trusted_checkout_and_retry_reuses_allocation(self):
        self.git("init", "--bare", str(self.root / "origin.git"))
        self.git("remote", "add", "origin", str(self.root / "origin.git"))
        self.git("push", "origin", "master", "refs/tags/v1.0.0")
        trusted = self.git("rev-parse", "HEAD")
        self.git("checkout", "-b", "feature")
        Path("pyproject.toml").write_text(
            Path("pyproject.toml")
            .read_text()
            .replace(
                'tag_format = "v$version"',
                'tag_format = "release-$version"\nlegacy_tag_formats = ["v$version"]',
            )
        )
        Path("package.py").write_text("value = 3\n")
        self.commit("feat: candidate")
        source = self.git("rev-parse", "HEAD")
        self.git("push", "origin", "feature")
        self.git("checkout", "master")
        registry = MagicMock()
        registry.qlty.files.return_value = Registry().baseline
        registry.pypi.files.return_value = Registry().baseline
        registry.versions.return_value = set()
        environment = {
            "GITHUB_RUN_ID": "42",
            "GITHUB_RUN_ATTEMPT": "1",
            "RUNNER_TEMP": str(self.root),
        }
        with (
            patch.dict(os.environ, environment),
            patch.object(self.github, "request", return_value={"sha": source}) as api,
            patch.object(planning, "Registries", return_value=registry),
            patch.object(release, "outputs") as output,
        ):
            release.inspect(self.github, "", "master", "feature", "2.0.0", "alpha")
        api.assert_called_once_with("GET", "commits/feature")
        assert_outputs(output, resume_tag="", source_sha=source, next_version="2.0.0a1")
        self.assertEqual(self.git("rev-parse", "HEAD"), trusted)
        allocation = (self.root / "library-allocation" / "allocation.json").read_bytes()
        store = (
            ArtifactAPI()
            .add("library-allocation", {"allocation.json": allocation})
            .receipt()
        )
        # The branch moves after allocation; a run retry must not resolve it again.
        self.git("checkout", "feature")
        Path("package.py").write_text("value = 4\n")
        self.commit("feat: later change")
        self.git("push", "origin", "feature")
        self.git("checkout", "master")
        with (
            patch.dict(
                os.environ,
                dict(environment, GITHUB_RUN_ATTEMPT="2", DISPATCH_TOKEN="test"),
            ),
            patch.object(
                release.GitHub,
                "request",
                side_effect=store.request,
            ) as api,
            patch.object(release, "outputs") as output,
        ):
            release.inspect(self.github, "", "master", "feature", "2.0.0", "alpha")
        assert_outputs(
            output,
            resume_tag="",
            source_sha=source,
            next_version="2.0.0a1",
            allocation_retained="true",
        )
        self.assertFalse(any("commits/" in path for _, path in store.operations))
        self.assertEqual(self.git("rev-parse", "HEAD"), trusted)

    def test_invalid_dispatch_inputs_fail(self):
        for inputs in [
            ("feature", "2.0.0", "preview"),
            ("", "2.0.0", "alpha"),
            ("feature", "2.0.0a1", "alpha"),
            ("feature", "02.0.0", "rc"),
        ]:
            with self.subTest(inputs=inputs), self.assertRaises(release.CheckError):
                planning.prerelease_inputs(*inputs)
