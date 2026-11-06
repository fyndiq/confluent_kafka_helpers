"""Post-upload visibility waits preserve the retained release and fail closed."""

import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import library_release as release
import release_context as context
from release_test_support import LifecycleFixture


class Clock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class RegistryVisibilityTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        for name in ("monotonic", "sleep"):
            replacement = patch.object(context, name, getattr(self.clock, name))
            replacement.start()
            self.addCleanup(replacement.stop)
        self.candidate = SimpleNamespace(
            package="example",
            version="1.0.1",
            artifacts={"example-1.0.1.whl": "wheel", "example-1.0.1.tar.gz": "sdist"},
        )

    def test_missing_files_become_visible_without_uploading(self):
        registry = Mock()
        registry.files.side_effect = [
            {},
            {"example-1.0.1.whl": "wheel"},
            self.candidate.artifacts,
        ]
        context.wait_for_registry(registry, self.candidate, "qlty")
        self.assertEqual(self.clock.sleeps, [2, 4])
        self.assertEqual(registry.files.call_count, 3)

    def test_timeout_reports_registry_missing_files_and_elapsed_wait(self):
        for label in ("qlty", "PyPI"):
            with self.subTest(label=label):
                self.clock.now = 0
                self.clock.sleeps.clear()
                registry = Mock()
                registry.files.return_value = {"example-1.0.1.whl": "wheel"}
                with self.assertRaises(release.CheckError) as raised:
                    context.wait_for_registry(registry, self.candidate, label)
                message = str(raised.exception)
                self.assertIn(label, message)
                self.assertIn("example-1.0.1.tar.gz", message)
                self.assertNotIn("example-1.0.1.whl", message)
                self.assertIn("120", message)
                self.assertIn("resume the same tag", message)
                self.assertEqual(self.clock.now, 120)
                self.assertEqual(self.clock.sleeps, [2, 4, 8, 16, 30, 30, 30])
                self.assertEqual(registry.files.call_count, 7)

    def test_slow_read_uses_remaining_deadline_and_reports_actual_elapsed(self):
        registry = Mock()

        def read(*args):
            self.clock.now += 119 if registry.files.call_count == 1 else 60
            return {}

        registry.files.side_effect = read
        with self.assertRaisesRegex(release.CheckError, "120"):
            context.wait_for_registry(registry, self.candidate, "qlty")
        self.assertEqual(self.clock.sleeps, [1])
        self.assertEqual(registry.files.call_count, 1)

        self.clock.now = 0

        # A read started within the window may finish after its deadline.
        def slow(*args):
            self.clock.now += 62
            return {}

        registry.files.side_effect = slow
        with self.assertRaisesRegex(release.CheckError, "126"):
            context.wait_for_registry(registry, self.candidate, "PyPI")
        self.assertEqual(self.clock.now, 126)

    def test_conflicting_or_unexpected_files_fail_without_sleep(self):
        for found in ({"example-1.0.1.whl": "wrong"}, {"unexpected.whl": "other"}):
            with self.subTest(found=found):
                registry = Mock()
                registry.files.return_value = found
                with self.assertRaisesRegex(release.CheckError, "differs"):
                    context.wait_for_registry(registry, self.candidate, "qlty")
                self.assertEqual(registry.files.call_count, 1)
                self.assertEqual(self.clock.sleeps, [])

    def test_read_errors_propagate_without_sleep(self):
        for error in (
            release.CheckError("Registry read failed"),
            ValueError("invalid"),
        ):
            with self.subTest(error=type(error)):
                registry = Mock()
                registry.files.side_effect = error
                with self.assertRaises(type(error)) as raised:
                    context.wait_for_registry(registry, self.candidate, "qlty")
                self.assertIs(raised.exception, error)
                self.assertEqual(self.clock.sleeps, [])

    def test_final_completeness_check_names_each_registry_without_waiting(self):
        for label, incomplete in (("qlty", "qlty"), ("PyPI", "pypi")):
            with self.subTest(label=label):
                registries = SimpleNamespace(qlty=Mock(), pypi=Mock())
                registries.qlty.files.return_value = self.candidate.artifacts
                registries.pypi.files.return_value = self.candidate.artifacts
                getattr(registries, incomplete).files.return_value = {
                    "example-1.0.1.whl": "wheel"
                }
                with self.assertRaises(release.CheckError) as raised:
                    context.reconcile_registries(
                        registries, self.candidate, complete=True
                    )
                self.assertIn(label, str(raised.exception))
                self.assertIn("example-1.0.1.tar.gz", str(raised.exception))
                self.assertEqual(self.clock.sleeps, [])


class PublicationVisibilityTests(LifecycleFixture, unittest.TestCase):
    def delayed_visibility(self, destination):
        clock = Clock()
        original_files = destination.files
        delayed_reads = 0

        def files(package, version):
            nonlocal delayed_reads
            found = original_files(package, version)
            if version == self.manifest["version"] and found and delayed_reads < 2:
                delayed_reads += 1
                return {} if delayed_reads == 1 else dict(list(found.items())[:1])
            return found

        originals = {path.name: path.read_bytes() for path in self.directory.iterdir()}
        with (
            patch.object(context, "monotonic", clock.monotonic),
            patch.object(context, "sleep", clock.sleep),
            patch.object(destination, "files", side_effect=files),
            patch.object(release, "checked", wraps=release.checked) as checked,
            patch.object(subprocess, "run", side_effect=self.child_command) as child,
        ):
            self.publish()
        self.assertEqual(clock.sleeps, [2, 4])
        self.assertEqual(self.publish_calls, 1)
        self.assertEqual(
            [call.args[:2] for call in checked.call_args_list if call.args[0] == "uv"],
            [("uv", "publish")],
        )
        self.assertEqual(
            [
                call.args[0][:2]
                for call in child.call_args_list
                if call.args[0][0] == "uv"
            ],
            [["uv", "publish"]],
        )
        self.assertEqual(
            {name: (self.directory / name).read_bytes() for name in originals},
            originals,
        )
        self.assertEqual(release.remote("refs/tags/v1.0.1^{}"), self.sha)
        self.assertFalse(self.github.records[0]["draft"])

    def test_qlty_upload_waits_for_delayed_files(self):
        self.delayed_visibility(self.registry)

    def test_pypi_upload_waits_for_delayed_files(self):
        self.delayed_visibility(self.pypi)
