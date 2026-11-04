"""Failure injection at publication boundaries, using real atomic Git pushes."""

import copy
import json
import os
import subprocess
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import library_release as release
import release_candidate
from release_test_support import LifecycleFixture


class LifecycleTests(LifecycleFixture, unittest.TestCase):
    def test_prerelease_partial_upload_retry_preserves_originals_and_source(self):
        self.prerelease_candidate()
        self.advance()
        moved_source = release.remote("refs/heads/master")
        before = {p.name: p.read_bytes() for p in self.directory.iterdir()}
        self.fail_publish = True
        with self.assertRaisesRegex(release.CheckError, "uv publish failed"):
            self.publish()
        tag = release.remote("refs/tags/v2.0.0a1")
        self.assertTrue(self.github.records[0]["prerelease"])
        self.fail_publish = False
        self.publish("v2.0.0a1")
        self.assertEqual(release.remote("refs/tags/v2.0.0a1"), tag)
        self.assertEqual(release.remote("refs/heads/master"), moved_source)
        self.assertTrue(self.github.records[0]["prerelease"])
        self.assertFalse(self.github.records[0]["draft"])
        self.assertEqual(
            before,
            {
                p.name: p.read_bytes()
                for p in self.directory.iterdir()
                if p.name != "uv-publish.toml"
            },
        )

    def test_prerelease_bundle_recovers_source_after_feature_ref_disappears(self):
        self.prerelease_candidate()
        self.git("push", "--force", "origin", "v1.0.0:refs/heads/master")
        publisher = self.root / "publisher"
        self.git("clone", "--no-local", str(self.origin), str(publisher))
        os.chdir(publisher)
        self.assertNotEqual(
            self.real_run("git", "cat-file", "-e", self.source).returncode, 0
        )
        real_output = release.git_output

        def without_source_fetch(*args):
            self.assertNotEqual(args, ("fetch", "origin", self.source))
            return real_output(*args)

        with patch.object(release, "git_output", side_effect=without_source_fetch):
            self.publish()
        self.assertEqual(
            release.remote("refs/heads/master"), self.git("rev-parse", "v1.0.0")
        )
        self.assertEqual(release.remote("refs/tags/v2.0.0a1^{}"), self.sha)

    def test_prerelease_publishes_subdirectory_snapshot(self):
        self.prerelease_candidate("library")
        os.chdir(self.repo / "library")
        self.publish()
        self.assertEqual(release.remote("refs/heads/master"), self.source)
        self.assertTrue(self.github.records[0]["prerelease"])
        self.assertEqual(self.publish_calls, 1)

    def test_stable_intent_resuming_prerelease_dispatches_default_branch_checks(self):
        self.prerelease_candidate()
        self.github.fail_complete = True
        with self.assertRaisesRegex(release.CheckError, "completion"):
            self.publish()
        self.github.fail_complete = False
        self.advance()
        branch = release.remote("refs/heads/master")
        environment = {
            "FOLLOWUP_STABLE": "true",
            "DISPATCH_TOKEN": "test",
            "GITHUB_WORKFLOW_REF": "fyndiq/example/.github/workflows/release.yml@refs/heads/master",
        }
        with (
            patch.dict(os.environ, environment),
            patch.object(release.GitHub, "request") as dispatch,
            patch.object(release, "outputs") as output,
        ):
            self.publish("v2.0.0a1")
        self.assertEqual(
            dispatch.call_args.args[2],
            {"ref": "master", "inputs": {"resume_tag": "", "release_retry": "0"}},
        )
        self.assertEqual(output.call_args.kwargs["prerelease"], "true")
        self.assertEqual(release.remote("refs/heads/master"), branch)
        # Direct prerelease dispatch and explicit recovery have no stable follow-up.
        with (
            patch.dict(os.environ, {"FOLLOWUP_STABLE": "false"}),
            patch.object(release.GitHub, "request") as dispatch,
        ):
            self.publish("v2.0.0a1")
        dispatch.assert_not_called()
        self.assertEqual(release.remote("refs/heads/master"), branch)

    def test_prerelease_completed_resume_rejects_changed_github_flag(self):
        self.prerelease_candidate()
        self.publish()
        self.github.records[0]["prerelease"] = False
        with self.assertRaisesRegex(release.CheckError, "prerelease state"):
            self.publish("v2.0.0a1")
        self.assertEqual(self.publish_calls, 1)

    def test_prerelease_rejects_changed_source_files(self):
        self.prerelease_candidate()
        Path("source.py").write_text("value = 99\n")
        self.git("commit", "--amend", "-am", "chore: release snapshot")
        # A direct-child snapshot still cannot change packaged source.
        self.manifest["release_sha"] = self.git("rev-parse", "HEAD")
        self.git(
            "update-ref", "refs/library-release/candidate", self.manifest["release_sha"]
        )
        self.git(
            "bundle",
            "create",
            str(self.directory / "release.bundle"),
            "refs/library-release/candidate",
            "^" + self.source,
        )
        self.manifest["retained"]["release.bundle"] = release.digest(
            self.directory / "release.bundle"
        )
        self.write_manifest()
        with self.assertRaisesRegex(release.CheckError, "only project version"):
            self.publish()
        self.assertEqual(self.publish_calls, 0)

    def test_trusted_inspection_identity_rejects_substitution(self):
        with self.assertRaisesRegex(release.CheckError, "trusted inspected identity"):
            release.publish(
                self.github,
                self.directory,
                "",
                "master",
                0,
                registry_factory=lambda _: self.registries,
                followup_stable=os.environ.get("FOLLOWUP_STABLE") == "true",
                source_sha="0" * 40,
                version="2.0.0a1",
            )
        self.assertEqual(self.github.records, [])

    def test_success_retains_before_atomic_push_and_completes_after_registry(self):
        self.publish()
        self.assertFalse(self.github.records[0]["draft"])
        self.assertEqual(self.registry.current, self.manifest["artifacts"])
        self.assertEqual(
            self.git("ls-remote", "origin", "refs/heads/master").split()[0], self.sha
        )
        self.assertEqual(self.github.records[0]["target_commitish"], self.source)
        self.assertEqual(self.publish_calls, 1)

    def test_partial_registry_retry_keeps_tag_artifacts_and_version(self):
        self.fail_publish = True
        with self.assertRaisesRegex(release.CheckError, "uv publish failed"):
            self.publish()
        tag = release.remote("refs/tags/v1.0.1")
        originals = copy.deepcopy(self.github.data)
        self.fail_publish = False
        self.publish()
        self.assertEqual(release.remote("refs/tags/v1.0.1"), tag)
        self.assertEqual(self.github.data, originals)
        self.assertEqual(self.publish_calls, 2)

    def test_github_completion_retry_does_not_upload_again(self):
        self.github.fail_complete = True
        with self.assertRaisesRegex(release.CheckError, "completion"):
            self.publish()
        tag = release.remote("refs/tags/v1.0.1")
        self.github.fail_complete = False
        self.publish()
        self.assertEqual(self.publish_calls, 1)
        self.assertEqual(release.remote("refs/tags/v1.0.1"), tag)

    def test_completed_resume_verifies_registry_without_republishing(self):
        self.publish()
        self.publish()
        self.publish("v1.0.1")
        self.assertEqual(self.publish_calls, 1)

    def test_atomic_failure_resumes_original_bundle(self):
        self.fail_push = True
        with self.assertRaisesRegex(release.CheckError, "Atomic push failed"):
            self.publish()
        self.assertEqual(release.remote("refs/heads/master"), self.source)
        self.assertEqual(release.remote("refs/tags/v1.0.1"), "")
        # A new runner has no local tag or prepared branch.
        self.git("tag", "-d", "v1.0.1")
        self.git("checkout", "--detach", self.source)
        self.fail_push = False
        self.publish("v1.0.1")
        self.assertEqual(release.remote("refs/heads/master"), self.sha)

    def test_lost_successful_push_response_preserves_draft_for_same_tag_retry(self):
        real = self.command

        def ambiguous(*args):
            result = real(*args)
            if args[:2] == ("git", "push"):
                return subprocess.CompletedProcess(args, 1, "", "lost response")
            return result

        with (
            patch.object(release, "run", side_effect=ambiguous),
            self.assertRaisesRegex(release.CheckError, "Atomic push failed"),
        ):
            self.publish()
        self.assertEqual(release.remote("refs/heads/master"), self.sha)
        self.assertTrue(self.github.records[0]["draft"])
        self.publish("v1.0.1")
        self.assertFalse(self.github.records[0]["draft"])
        self.assertEqual(self.publish_calls, 1)

    def test_tag_collision_does_not_create_draft_or_registry_files(self):
        self.git("tag", "-a", "v1.0.1", self.source, "-m", "existing")
        self.git("push", "origin", "refs/tags/v1.0.1")
        with self.assertRaisesRegex(release.CheckError, "collision"):
            self.publish()
        self.assertEqual(self.github.records, [])
        self.assertEqual(self.publish_calls, 0)

    def test_registry_mismatch_fails_without_replacing_files(self):
        self.github.fail_complete = True
        with self.assertRaises(release.CheckError):
            self.publish()
        self.registry.current[next(iter(self.registry.current))] = "different"
        with self.assertRaisesRegex(release.CheckError, "differs"):
            self.publish("v1.0.1")
        self.assertEqual(self.publish_calls, 1)

    def test_retained_corruption_fails_before_publication(self):
        self.github.fail_complete = True
        with self.assertRaises(release.CheckError):
            self.publish()
        self.github.data[1, "notes.md"] = b"changed"
        with (
            patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
            patch.object(
                release.GitHub,
                "request",
                side_effect=release.CheckError("missing originals"),
            ),
            self.assertRaises(release.CheckError),
        ):
            self.publish("v1.0.1")
        self.assertEqual(self.publish_calls, 1)

    def test_corrupt_draft_originals_fail_without_actions_fallback(self):
        self.github.fail_upload = "release.bundle"
        with self.assertRaises(release.CheckError):
            self.publish()
        for name, data in (("manifest.json", b"{}"), ("notes.md", b"corrupt")):
            with self.subTest(name=name):
                original = self.github.data[1, name]
                self.github.data[1, name] = data
                with (
                    patch.object(release.GitHub, "request") as request,
                    self.assertRaisesRegex(release.CheckError, "SHA256 differs"),
                ):
                    self.publish("v1.0.1")
                request.assert_not_called()
                self.github.data[1, name] = original

    def test_draft_metadata_identity_fails_without_actions_fallback(self):
        self.github.fail_upload = "manifest.json"
        with self.assertRaises(release.CheckError):
            self.publish()
        self.github.records[0]["tag_name"] = "v1.0.2"
        with (
            patch.object(release.GitHub, "request") as request,
            self.assertRaisesRegex(release.CheckError, "tag differs"),
        ):
            self.publish("v1.0.2")
        request.assert_not_called()

    def test_expected_candidate_mismatch_fails_before_recovery_changes_files(self):
        self.github.fail_upload = "manifest.json"
        with self.assertRaises(release.CheckError):
            self.publish()
        self.manifest["transport"]["run_id"] = "43"
        self.write_manifest()
        before = {path.name: path.read_bytes() for path in self.directory.iterdir()}
        with (
            patch.object(release.GitHub, "request") as request,
            self.assertRaisesRegex(release.CheckError, "Candidate differs"),
        ):
            self.publish()
        request.assert_not_called()
        self.assertEqual(self.github.data, {})
        self.assertEqual(
            before, {path.name: path.read_bytes() for path in self.directory.iterdir()}
        )

    def test_completed_manifest_mismatch_fails_before_missing_asset_fallback(self):
        self.publish()
        manifest = json.loads(self.github.data[1, "manifest.json"])
        manifest["transport"]["run_id"] = "43"
        self.github.data[1, "manifest.json"] = json.dumps(manifest).encode()
        del self.github.data[1, "release.bundle"]
        with (
            patch.object(release.GitHub, "request") as request,
            self.assertRaisesRegex(release.CheckError, "Tag pending metadata differs"),
        ):
            self.publish("v1.0.1")
        request.assert_not_called()

    def test_malformed_draft_metadata_fails_before_transport(self):
        self.github.fail_upload = "manifest.json"
        with self.assertRaises(release.CheckError):
            self.publish()
        self.github.records[0]["body"] = release_candidate.METADATA + "invalid -->"
        with (
            patch.object(release.GitHub, "request") as request,
            self.assertRaisesRegex(release.CheckError, "Invalid retained candidate"),
        ):
            self.publish("v1.0.1")
        request.assert_not_called()

    def test_atomic_push_failure_accepts_same_publisher_inputs_on_retry(self):
        self.fail_push = True
        with self.assertRaisesRegex(release.CheckError, "Atomic push failed"):
            self.publish()
        self.fail_push = False
        self.publish()
        self.assertFalse(self.github.records[0]["draft"])

    def test_missing_originals_never_rebuild(self):
        self.github.fail_complete = True
        with self.assertRaises(release.CheckError):
            self.publish()
        del self.github.data[1, "release.bundle"]
        with (
            patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
            patch.object(
                release.GitHub,
                "request",
                side_effect=release.CheckError("missing originals"),
            ),
            self.assertRaises(release.CheckError),
        ):
            self.publish("v1.0.1")
        self.assertEqual(self.publish_calls, 1)

    def test_stale_source_dispatches_revalidation_without_publishing(self):
        self.advance()
        with (
            patch.dict(
                os.environ,
                {
                    "DISPATCH_TOKEN": "test",
                    "GITHUB_WORKFLOW_REF": "fyndiq/example/.github/workflows/release.yml@refs/heads/master",
                },
            ),
            patch.object(release.GitHub, "request") as dispatch,
        ):
            self.publish()
        self.assertEqual(self.github.records, [])
        self.assertEqual(self.publish_calls, 0)
        self.assertEqual(release.remote("refs/tags/v1.0.1"), "")
        self.assertEqual(dispatch.call_args.args[2]["inputs"]["release_retry"], "1")

    def test_stale_retry_cap_preserves_originals(self):
        self.advance()
        with self.assertRaisesRegex(release.CheckError, "repeatedly"):
            self.publish(retry=3)
        self.assertTrue(self.github.records[0]["draft"])
        self.assertEqual(self.publish_calls, 0)

    def test_branch_race_during_atomic_push_restarts_without_tag(self):
        real = self.command

        def racing(*args):
            if args[:2] == ("git", "push"):
                self.advance()
            return real(*args)

        with (
            patch.object(release, "run", side_effect=racing),
            patch.dict(
                os.environ,
                {
                    "DISPATCH_TOKEN": "test",
                    "GITHUB_WORKFLOW_REF": "fyndiq/example/.github/workflows/release.yml@refs/heads/master",
                },
            ),
            patch.object(release.GitHub, "request") as dispatch,
        ):
            self.publish()
        self.assertEqual(release.remote("refs/tags/v1.0.1"), "")
        self.assertEqual(self.publish_calls, 0)
        dispatch.assert_called_once()

    def test_unverified_legacy_baseline_prevents_push(self):
        self.registry.baseline.clear()
        with self.assertRaisesRegex(release.CheckError, "baseline"):
            self.publish()
        self.assertEqual(release.remote("refs/tags/v1.0.1"), "")

    def test_completed_automation_baseline_requires_its_original_hashes(self):
        self.github.records.append(
            {"id": 99, "tag_name": "v1.0.0", "draft": False, "name": "1.0.0"}
        )
        original = {
            "repository": self.github.repository,
            "version": "1.0.0",
            "artifacts": dict.fromkeys(self.registry.baseline, "different"),
        }
        self.github.data[99, "manifest.json"] = json.dumps(original).encode()
        with self.assertRaisesRegex(release.CheckError, "baseline differs"):
            self.publish()
        self.assertEqual(release.remote("refs/tags/v1.0.1"), "")

    def test_baseline_manifest_cannot_redefine_original_tagged_hashes(self):
        import hashlib

        original = {
            **self.manifest,
            "version": "1.0.0",
            "tag": "v1.0.0",
            "artifacts": dict.fromkeys(self.registry.baseline, "a" * 64),
        }
        self.registry.baseline = original["artifacts"].copy()
        self.pypi.baseline = original["artifacts"].copy()
        contents = json.dumps(original).encode()
        sha = self.git("rev-parse", "v1.0.0")
        self.git("tag", "-d", "v1.0.0")
        self.git(
            "tag",
            "-a",
            "v1.0.0",
            sha,
            "-m",
            release.MARKER + "\n" + hashlib.sha256(contents).hexdigest(),
        )
        baseline = {"id": 99, "tag_name": "v1.0.0", "draft": False, "name": "1.0.0"}
        self.github.data[99, "manifest.json"] = contents
        release.verify_baseline(
            self.github, self.registries, "example", "1.0.0", "v1.0.0", [baseline]
        )
        self.registry.baseline["example-1.0.0.tar.gz"] = "changed"
        self.github.data[99, "manifest.json"] = json.dumps(original).encode()
        with self.assertRaisesRegex(release.CheckError, "baseline differs"):
            release.verify_baseline(
                self.github, self.registries, "example", "1.0.0", "v1.0.0", [baseline]
            )

    def test_automatic_pending_resume_checks_newer_source_after_completion(self):
        self.github.fail_complete = True
        with self.assertRaises(release.CheckError):
            self.publish()
        Path("new-after-release.txt").write_text("newer merge\n")
        self.git("add", "new-after-release.txt")
        self.git("commit", "-m", "feat: next change")
        self.git("push", "origin", "HEAD:master")
        self.github.fail_complete = False
        with (
            patch.dict(
                os.environ,
                {
                    "FOLLOWUP_STABLE": "true",
                    "DISPATCH_TOKEN": "test",
                    "GITHUB_WORKFLOW_REF": "fyndiq/example/.github/workflows/release.yml@refs/heads/master",
                },
            ),
            patch.object(release.GitHub, "request") as dispatch,
        ):
            self.publish("v1.0.1")
        self.assertEqual(dispatch.call_args.args[2]["inputs"]["release_retry"], "0")
        self.assertEqual(self.publish_calls, 1)

    def test_pending_prevents_another_bump(self):
        self.fail_push = True
        with self.assertRaises(release.CheckError):
            self.publish()
        self.manifest["tag"] = "v1.0.2"
        self.write_manifest()
        with self.assertRaisesRegex(release.CheckError, "pending"):
            self.publish()

    def test_unnamed_release_without_originals_fails_without_artifact_lookup(self):
        with (
            patch.object(release.GitHub, "request") as request,
            self.assertRaisesRegex(
                release.CheckError, "Required originals are missing"
            ),
        ):
            release.retained_candidate(self.github, {"id": 99, "name": None})
        request.assert_not_called()

    def test_incomplete_draft_upload_recovers_original_workflow_artifact(self):
        archive = BytesIO()
        with zipfile.ZipFile(archive, "w") as zipped:
            for path in self.directory.iterdir():
                zipped.writestr(path.name, path.read_bytes())
        self.github.fail_upload = "release.bundle"
        with (
            patch.dict(
                os.environ,
                {
                    "GITHUB_RUN_ID": "42",
                    "GITHUB_RUN_ATTEMPT": "2",
                    "CANDIDATE_RUN_ATTEMPT": "1",
                },
            ),
            self.assertRaises(release.CheckError),
        ):
            self.publish()
        self.assertEqual(self.github.records[0]["name"], "1.0.1")
        self.github.fail_upload = ""
        fresh = self.root / "recovered"
        self.directory = fresh

        def artifact(method, path, data=None, raw=False):
            if raw:
                self.assertEqual(path, "actions/artifacts/123/zip")
                return archive.getvalue()
            self.assertEqual(path, "actions/runs/42/artifacts?per_page=100&page=1")
            return {
                "artifacts": [
                    {"name": "library-candidate-1", "id": 123, "expired": False},
                    {"name": "library-candidate-2", "id": 456, "expired": False},
                ]
            }

        with (
            patch.dict(os.environ, {"DISPATCH_TOKEN": "test"}),
            patch.object(release.GitHub, "request", side_effect=artifact),
        ):
            self.publish("v1.0.1")
        self.assertFalse(self.github.records[0]["draft"])
        self.assertEqual(self.publish_calls, 1)

    def test_retained_original_mismatch_is_never_overwritten(self):
        item = self.github.request("POST", "releases", {"draft": True})
        self.github.data[1, "manifest.json"] = b"different"
        with self.assertRaisesRegex(release.CheckError, "differs"):
            release.retain(
                self.github,
                item,
                self.directory,
                release.load(self.directory, self.github.repository),
            )
        self.assertEqual(self.github.data[1, "manifest.json"], b"different")

    def test_existing_unannotated_tag_rejected_on_resume(self):
        self.fail_push = True
        with self.assertRaises(release.CheckError):
            self.publish()
        self.git("tag", "-d", "v1.0.1")
        self.git("tag", "v1.0.1", self.sha)
        self.git("push", "origin", "refs/tags/v1.0.1")
        with self.assertRaisesRegex(release.CheckError, "annotated"):
            self.publish("v1.0.1")
