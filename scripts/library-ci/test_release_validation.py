"""Credential-safe transport and distribution validation."""

import tarfile
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError
from urllib.request import Request

import library_release as release
import release_context as context


class ValidationTests(unittest.TestCase):
    def test_github_errors_report_only_method_path_and_failure_category(self):
        url = "https://user:credential@uploads.github.com/repos/fyndiq/example/releases/1/assets?name=secret-query"
        failures = [
            (
                HTTPError(url, 403, "secret-reason", {}, BytesIO(b"secret-body")),
                "HTTP 403",
            ),
            (URLError("secret-network-detail"), "network failure"),
            (OSError("secret-os-detail"), "network failure"),
        ]
        github = release.GitHub("fyndiq/example", "secret-token")
        for error, category in failures:
            with (
                self.subTest(category=category),
                patch.object(context, "build_opener") as opener,
                self.assertRaises(release.CheckError) as raised,
            ):
                opener.return_value.open.side_effect = error
                github.request("POST", url, b"secret-request-body")
            self.assertEqual(
                str(raised.exception),
                f"GitHub API POST /repos/fyndiq/example/releases/1/assets failed ({category}).",
            )

    def test_github_invalid_response_does_not_expose_response_or_query(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"secret-invalid-json"
        with (
            patch.object(context, "build_opener") as opener,
            self.assertRaises(release.CheckError) as raised,
        ):
            opener.return_value.open.return_value = response
            release.GitHub("fyndiq/example", "secret-token").request(
                "GET", "releases?secret-query"
            )
        self.assertEqual(
            str(raised.exception),
            "GitHub API GET /repos/fyndiq/example/releases failed (invalid response).",
        )

    def test_registry_matches_actual_file_hashes_instead_of_index_claims(self):
        import hashlib

        registry = context.Registry(
            "https://europe-north1-python.pkg.dev/fyndiq/qlty/simple/"
        )
        page = b'<a href="../../files/example-1.0.1-py3-none-any.whl#sha256=wrong">wheel</a><a href="../../files/example-1.0.1.tar.gz">sdist</a><a href="../../files/example-1.0.0.tar.gz">old</a>'
        with patch.object(
            registry, "read", side_effect=[page, b"wheel bytes", b"sdist bytes"]
        ) as read:
            found = registry.files("example", "1.0.1")
        self.assertEqual(
            found,
            {
                "example-1.0.1-py3-none-any.whl": hashlib.sha256(
                    b"wheel bytes"
                ).hexdigest(),
                "example-1.0.1.tar.gz": hashlib.sha256(b"sdist bytes").hexdigest(),
            },
        )
        self.assertEqual(read.call_count, 3)

    def test_redirect_strips_github_authorization(self):
        request = Request(
            "https://api.github.com/asset", headers={"Authorization": "Bearer test"}
        )
        redirected = context.Redirects().redirect_request(
            request, None, 302, "", {}, "https://objects.githubusercontent.com/signed"
        )
        self.assertIsNone(redirected.get_header("Authorization"))

    def test_registry_cross_host_and_insecure_redirect_rejected(self):
        request = Request("https://europe-north1-python.pkg.dev/path")
        for url in (
            "https://example.com/file",
            "http://europe-north1-python.pkg.dev/file",
        ):
            with self.subTest(url=url), self.assertRaises(release.CheckError):
                context.Redirects(registry=True).redirect_request(
                    request, None, 302, "", {}, url
                )

    def test_distribution_metadata_must_match_filename(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            wheel = directory / "example-1.0.1-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr(
                    "example-1.0.1.dist-info/METADATA",
                    "Name: example\nVersion: 9.9.9\n",
                )
            with self.assertRaisesRegex(release.CheckError, "metadata"):
                release.validate_distribution(wheel, "example", "1.0.1")
            sdist = directory / "example-1.0.1.tar.gz"
            with tarfile.open(sdist, "w:gz") as archive:
                contents = b"Name: example\nVersion: 1.0.1\n"
                entry = tarfile.TarInfo("example-1.0.1/PKG-INFO")
                entry.size = len(contents)
                archive.addfile(entry, BytesIO(contents))
            release.validate_distribution(sdist, "example", "1.0.1")
