"""Ambient Actions reruns cannot change local fixture behavior or output files."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from release_context import GitHub


class FixtureIsolationTests(unittest.TestCase):
    def test_rerun_context_cannot_trigger_recovery_or_write_runner_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "outputs"
            summary = root / "summary"
            output.write_text("existing output\n")
            summary.write_text("existing summary\n")
            ambient = {
                "GITHUB_RUN_ID": "999",
                "GITHUB_RUN_ATTEMPT": "2",
                "GITHUB_OUTPUT": str(output),
                "GITHUB_STEP_SUMMARY": str(summary),
                "RUNNER_TEMP": directory,
                "FOLLOWUP_STABLE": "true",
            }
            suite = unittest.defaultTestLoader.loadTestsFromNames(
                [
                    "test_release_plan.LifecycleTests.test_unnamed_draft_does_not_block_inspection_or_publication",
                    "test_release_prepare.PreparationTests.test_invalid_full_release_range_fails_before_build",
                    "test_release_prepare.PreparationTests.test_no_increment_creates_no_candidate",
                ]
            )
            with (
                patch.dict(os.environ, ambient),
                patch.object(
                    GitHub,
                    "request",
                    side_effect=AssertionError("Unexpected API request"),
                ) as request,
            ):
                result = unittest.TestResult()
                suite.run(result)
                self.assertTrue(result.wasSuccessful(), result.errors + result.failures)
                self.assertEqual({key: os.environ[key] for key in ambient}, ambient)
            request.assert_not_called()
            self.assertEqual(output.read_text(), "existing output\n")
            self.assertEqual(summary.read_text(), "existing summary\n")
