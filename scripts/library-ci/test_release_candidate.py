"""Validate candidate identity before any publication or fallback transport."""

import json
import unittest

from library_ci import CheckError
from release_candidate import MARKER, Candidate


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "schema": MARKER,
            "repository": "fyndiq/example",
            "package": "example",
            "version": "1.0.1",
            "source_sha": "a" * 40,
            "release_sha": "b" * 40,
            "tag": "v1.0.1",
            "baseline": "v1.0.0",
            "baseline_version": "1.0.0",
            "index": "https://europe-north1-python.pkg.dev/fyndiq/qlty/simple/",
            "transport": {"run_id": "42", "attempt": "1"},
            "artifacts": {
                "example-1.0.1-py3-none-any.whl": "c" * 64,
                "example-1.0.1.tar.gz": "d" * 64,
            },
            "retained": {"notes.md": "e" * 64, "release.bundle": "f" * 64},
        }

    def parse(self):
        return Candidate.parse(json.dumps(self.manifest).encode(), "fyndiq/example")

    def test_valid_candidate_keeps_original_bytes_and_transport(self):
        candidate = self.parse()
        self.assertEqual(candidate.original, json.dumps(self.manifest).encode())
        self.assertEqual(candidate.transport.run_id, "42")
        self.assertEqual(candidate.transport.attempt, "1")

    def test_filename_identity_must_match_candidate_package_and_version(self):
        for field, value in (("package", "another"), ("version", "1.0.2")):
            with self.subTest(field=field):
                original = self.manifest[field]
                self.manifest[field] = value
                with self.assertRaisesRegex(CheckError, "filename differs"):
                    self.parse()
                self.manifest[field] = original

    def test_invalid_package_name_fails_at_candidate_boundary(self):
        self.manifest["package"] = "invalid/package"
        with self.assertRaises(CheckError):
            self.parse()

    def test_transport_requires_actual_positive_run_and_attempt(self):
        for field in ("run_id", "attempt"):
            for value in ("0", "", "-1", "1.2", 1):
                with self.subTest(field=field, value=value):
                    original = self.manifest["transport"][field]
                    self.manifest["transport"][field] = value
                    with self.assertRaisesRegex(CheckError, "transport identity"):
                        self.parse()
                    self.manifest["transport"][field] = original
