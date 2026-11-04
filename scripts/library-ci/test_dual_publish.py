"""Cross-registry failure and recovery checks using retained bytes and real Git."""

import hashlib
import os
import subprocess
import unittest
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch

import library_ci
import library_release as release
from release_context import (
    PublicPyPI,
    PublicRedirects,
    reconcile_registries,
    verify_baseline,
)
from release_notifications import SCHEMA, announce
from release_plan import allocate_prerelease
from release_test_support import LifecycleFixture, registries
from test_release_notifications import Messages


class DualLifecycleTests(LifecycleFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.destinations = registries()
        self.destinations.qlty = self.registry
        self.public = self.destinations.pypi
        self.environment = patch.dict(os.environ, {"PYPI_API_TOKEN": "test-only-token"})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.child_run = subprocess.run
        self.public_calls = 0
        self.fail_public = False
        self.publisher = patch.object(subprocess, "run", side_effect=self.child)
        self.publisher.start()
        self.addCleanup(self.publisher.stop)

    def child(self, args, **kwargs):
        if args[:2] != ["uv", "publish"]:
            return self.child_run(args, **kwargs)
        self.public_calls += 1
        self.assertEqual(self.registry.current, self.manifest["artifacts"])
        self.assertEqual(kwargs["env"]["UV_PUBLISH_TOKEN"], "test-only-token")
        self.assertNotIn("PYPI_API_TOKEN", kwargs["env"])
        self.assertNotIn("test-only-token", args)
        for name, value in self.manifest["artifacts"].items():
            self.assertIn(str(self.directory / name), args)
            self.assertEqual(release.digest(self.directory / name), value)
        self.public.current.update(self.manifest["artifacts"])
        if self.fail_public:
            self.public.current.pop(next(iter(self.public.current)))
        return subprocess.CompletedProcess(
            args, int(self.fail_public), "", "secret output"
        )

    def publish(self, resume="", retry=0):
        release.publish(
            self.github,
            self.directory,
            resume,
            "master",
            retry,
            registry_factory=lambda _: self.destinations,
        )

    def test_partial_pypi_retry_retains_originals_and_does_not_reupload_qlty(self):
        originals = {p.name: p.read_bytes() for p in self.directory.iterdir()}
        self.fail_public = True
        with self.assertRaisesRegex(release.CheckError, "PyPI publication failed"):
            self.publish()
        tag = release.remote("refs/tags/v1.0.1")
        self.assertTrue(self.github.records[0]["draft"])
        self.fail_public = False
        self.publish("v1.0.1")
        self.assertEqual(self.publish_calls, 1)
        self.assertEqual(self.public_calls, 2)
        self.assertEqual(tag, release.remote("refs/tags/v1.0.1"))
        self.assertFalse(self.github.records[0]["draft"])
        self.assertEqual(
            originals,
            {
                p.name: p.read_bytes()
                for p in self.directory.iterdir()
                if p.name != "uv-publish.toml"
            },
        )

    def test_partial_qlty_failure_never_starts_pypi(self):
        self.fail_publish = True
        with self.assertRaises(release.CheckError):
            self.publish()
        self.assertEqual(self.public_calls, 0)
        self.assertTrue(self.github.records[0]["draft"])

    def test_public_collision_prevents_draft_and_tag(self):
        self.public.current.update(self.manifest["artifacts"])
        with self.assertRaisesRegex(release.CheckError, "already exists"):
            self.publish()
        self.assertFalse(self.github.records)
        self.assertEqual(release.remote("refs/tags/v1.0.1"), "")

    def test_completed_retry_requires_both_registries(self):
        self.publish()
        self.public.current[next(iter(self.public.current))] = "changed"
        with self.assertRaisesRegex(release.CheckError, "differs"):
            self.publish("v1.0.1")
        self.assertEqual(self.public_calls, 1)

    def test_missing_pypi_baseline_prevents_any_publication(self):
        self.public.baseline.clear()
        with self.assertRaisesRegex(release.CheckError, "baseline"):
            self.publish()
        self.assertEqual(self.publish_calls, 0)
        self.assertEqual(self.public_calls, 0)

    def test_stale_cleanup_preserves_candidate_if_only_public_files_exist(self):
        self.publish()
        candidate = release.load(self.directory, self.github.repository)
        self.registry.current.clear()
        with (
            patch.object(release, "remote", return_value=""),
            self.assertRaisesRegex(release.CheckError, "publication evidence"),
        ):
            release.stale_restart(
                self.github,
                self.github.records[0],
                candidate,
                self.destinations,
                "master",
                0,
            )
        self.assertTrue(self.github.records)

    def test_prerelease_also_publishes_both_destinations(self):
        self.prerelease_candidate()
        self.publish()
        self.assertEqual(self.public.current, self.manifest["artifacts"])
        self.assertEqual(self.public_calls, 1)
        self.assertTrue(self.github.records[0]["prerelease"])

    def test_announcement_requires_both_registry_publications(self):
        self.manifest["notification"] = SCHEMA
        self.write_manifest()
        self.publish()
        self.public.current.clear()
        slack = Messages()
        with self.assertRaisesRegex(release.CheckError, "incomplete"):
            announce(
                self.github,
                "v1.0.1",
                "C123",
                slack,
                registry_factory=lambda _: self.destinations,
            )
        self.assertFalse(slack.calls)

    def test_newer_release_is_blocked_after_public_failure(self):
        self.fail_public = True
        with self.assertRaises(release.CheckError):
            self.publish()
        self.manifest["tag"] = "v1.0.2"
        self.write_manifest()
        with self.assertRaisesRegex(release.CheckError, "pending"):
            self.publish()
        self.assertEqual(self.public_calls, 1)


class BoundaryTests(unittest.TestCase):
    def test_completion_cannot_merge_two_incomplete_registries(self):
        destinations = registries()
        destinations.qlty.current = {"one": "a"}
        destinations.pypi.current = {"two": "b"}
        candidate = SimpleNamespace(
            package="example", version="1.0.1", artifacts={"one": "a", "two": "b"}
        )
        with self.assertRaisesRegex(release.CheckError, "incomplete"):
            reconcile_registries(destinations, candidate, complete=True)

    def test_allocation_reserves_versions_in_either_registry(self):
        destinations = registries()
        destinations.qlty.versions = lambda _: {release.Version("2.0.0a2")}
        destinations.pypi.versions = lambda _: {release.Version("2.0.0a4")}
        metadata = {
            "project": {"name": "example", "version": "1.0.0"},
            "tool": {"commitizen": {"tag_format": "v$version"}},
        }
        with patch("release_plan.git.get_tags", return_value=[]):
            self.assertEqual(
                allocate_prerelease(metadata, destinations, "2.0.0", "alpha"), "2.0.0a5"
            )
        destinations.pypi.versions = lambda _: {release.Version("2.0.0")}
        with self.assertRaisesRegex(release.CheckError, "already exists"):
            allocate_prerelease(metadata, destinations, "2.0.0", "alpha")

    def test_public_reads_hash_downloaded_bytes_without_auth(self):
        registry = PublicPyPI()
        wheel = "example-1.0.1-py3-none-any.whl"
        page = f'<a href="https://files.pythonhosted.org/{wheel}#sha256=wrong">file</a>'
        requests = []

        class Response(BytesIO):
            pass

        class Opener:
            def open(self, request, timeout):
                requests.append(request)
                return Response(
                    page.encode()
                    if request.full_url.endswith("/example/")
                    else b"actual"
                )

        with patch("release_context.build_opener", return_value=Opener()):
            self.assertEqual(
                registry.files("example", "1.0.1"),
                {wheel: hashlib.sha256(b"actual").hexdigest()},
            )
        self.assertTrue(all(not r.has_header("Authorization") for r in requests))

    def test_public_reads_and_redirects_reject_foreign_or_credentialed_hosts(self):
        for url in [
            "https://evil.test/file",
            "http://pypi.org/simple/",
            "https://token@pypi.org/simple/",
            "https://pypi.org:8443/file",
        ]:
            with self.subTest(url=url), self.assertRaises(release.CheckError):
                PublicPyPI().read(url)
            with self.subTest(url=url), self.assertRaises(release.CheckError):
                PublicRedirects().redirect_request(None, None, 302, "", {}, url)

    def test_shared_subprocess_never_inherits_pypi_credentials(self):
        with (
            patch.dict(
                os.environ, {"PYPI_API_TOKEN": "secret", "UV_PUBLISH_TOKEN": "secret"}
            ),
            patch("library_ci.subprocess.run") as child,
        ):
            library_ci.run("uv", "publish", "--index", "qlty")
        self.assertNotIn("PYPI_API_TOKEN", child.call_args.kwargs["env"])
        self.assertNotIn("UV_PUBLISH_TOKEN", child.call_args.kwargs["env"])

    def test_bootstrap_exception_is_exact_and_requires_pinned_commit(self):
        destinations = registries()
        destinations.pypi.baseline = {}
        expected = {
            "confluent_kafka_helpers-1.6.0-py3-none-any.whl": "c9dce8d6f4647f702b7b1e0c6acd85f7e0c912ed901daca5ba3de82729e81a06"
        }
        destinations.pypi.files = lambda *_: expected
        github = SimpleNamespace(repository="fyndiq/confluent_kafka_helpers")
        with patch(
            "release_context.git_output",
            return_value="c28b13b173a0f01c668cd0f27383df34c64c6cd8",
        ):
            verify_baseline(
                github, destinations, "confluent-kafka-helpers", "1.6.0", "v1.6.0", []
            )
        with (
            patch("release_context.git_output", return_value="0" * 40),
            self.assertRaisesRegex(release.CheckError, "Legacy baseline"),
        ):
            verify_baseline(
                github, destinations, "confluent-kafka-helpers", "1.6.0", "v1.6.0", []
            )
        expected["unexpected.tar.gz"] = "wrong"
        with (
            patch(
                "release_context.git_output",
                return_value="c28b13b173a0f01c668cd0f27383df34c64c6cd8",
            ),
            self.assertRaisesRegex(release.CheckError, "Legacy baseline"),
        ):
            verify_baseline(
                github, destinations, "confluent-kafka-helpers", "1.6.0", "v1.6.0", []
            )
