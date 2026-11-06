"""Trusted GitHub, registry and shared release metadata operations."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tomllib
from html.parser import HTMLParser
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from commitizen import defaults, git
from commitizen.tags import TagRules
from library_ci import CheckError, git_output
from packaging.utils import (
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import Version
from release_candidate import (
    MARKER,
    Candidate,
    draft_candidate,
)


class Redirects(HTTPRedirectHandler):
    def __init__(self, registry=False):
        self.registry = registry

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        if urlsplit(newurl).scheme != "https":
            raise CheckError("Refusing an insecure credential-bearing redirect.")
        changed = urlsplit(request.full_url).netloc != urlsplit(newurl).netloc
        if changed and self.registry:
            raise CheckError("Registry redirected outside the configured host.")
        redirected = super().redirect_request(request, fp, code, msg, headers, newurl)
        if changed and redirected:
            redirected.remove_header("Authorization")
        return redirected


class GitHub:
    def __init__(self, repository, token=None):
        if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
            raise CheckError("Invalid GitHub repository.")
        self.repository = repository
        self.token = token or os.environ["GH_TOKEN"]

    def request(self, method, path, data=None, raw=False):
        url = (
            path
            if path.startswith("https://")
            else f"https://api.github.com/repos/{self.repository}/{path}"
        )
        # Only GitHub asset upload/download endpoints may receive the App token.
        if urlsplit(url).hostname not in {"api.github.com", "uploads.github.com"}:
            raise CheckError("Unexpected GitHub API host.")
        body = (
            data
            if isinstance(data, bytes)
            else json.dumps(data).encode()
            if data is not None
            else None
        )
        request = Request(
            url,
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/octet-stream"
                if raw
                else "application/vnd.github+json",
                "Content-Type": "application/octet-stream"
                if isinstance(data, bytes)
                else "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with build_opener(Redirects()).open(request, timeout=60) as response:
                result = response.read()
                return result if raw else json.loads(result) if result else None
        except HTTPError as error:
            failure = f"HTTP {error.code}"
        except (URLError, OSError):
            failure = "network failure"
        except ValueError:
            failure = "invalid response"
        endpoint = quote(urlsplit(url).path, safe="/%")
        safe_method = (
            method
            if method in {"GET", "POST", "PATCH", "DELETE", "PUT", "HEAD", "OPTIONS"}
            else "REQUEST"
        )
        raise CheckError(
            f"GitHub API {safe_method} {endpoint} failed ({failure})."
        ) from None

    def releases(self):
        result = []
        page = 1
        while True:
            batch = self.request("GET", f"releases?per_page=100&page={page}")
            result.extend(batch)
            if len(batch) < 100:
                return result
            page += 1

    def assets(self, release):
        result = []
        page = 1
        while True:
            batch = self.request(
                "GET", f"releases/{release['id']}/assets?per_page=100&page={page}"
            )
            result.extend(batch)
            if len(batch) < 100:
                return result
            page += 1

    def download(self, asset):
        return self.request("GET", asset["url"], raw=True)

    def upload(self, release, path):
        self.request(
            "POST",
            release["upload_url"].split("{")[0] + "?name=" + quote(path.name),
            path.read_bytes(),
        )


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        if tag == "a" and (href := dict(attrs).get("href")):
            self.urls.append(href)


class Registry:
    def __init__(self, index):
        # Restrict credentials to the infrastructure-managed registry.
        split = urlsplit(index)
        if (
            split.hostname != "europe-north1-python.pkg.dev"
            or split.path.rstrip("/") != "/fyndiq/qlty/simple"
        ):
            raise CheckError(
                "The qlty index must use the infrastructure-managed registry."
            )
        self.index = urlunsplit(
            ("https", split.hostname, split.path.rstrip("/") + "/", "", "")
        )

    def read(self, url, missing=False):
        import google.auth
        from google.auth.transport.requests import Request as AuthRequest

        if (
            urlsplit(url).scheme != "https"
            or urlsplit(url).netloc != urlsplit(self.index).netloc
        ):
            raise CheckError("Registry artifact points to an unexpected host.")
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(AuthRequest())
        auth = base64.b64encode(
            f"oauth2accesstoken:{credentials.token}".encode()
        ).decode()
        try:
            with build_opener(Redirects(registry=True)).open(
                Request(url, headers={"Authorization": f"Basic {auth}"}), timeout=60
            ) as response:
                return response.read()
        except HTTPError as error:
            if missing and error.code == 404:
                return b""
            raise CheckError(
                "Registry read failed; publication status is unknown."
            ) from None
        except (URLError, OSError):
            raise CheckError(
                "Registry read failed; publication status is unknown."
            ) from None

    def versions(self, package):
        page = urljoin(self.index, quote(package) + "/")
        parser = Links()
        parser.feed(self.read(page, missing=True).decode())
        versions = set()
        for link in parser.urls:
            name = unquote(Path(urlsplit(urljoin(page, link)).path).name)
            try:
                parsed = (
                    parse_wheel_filename(name)
                    if name.endswith(".whl")
                    else parse_sdist_filename(name)
                )
            except ValueError:
                continue
            if parsed[0] == package:
                versions.add(parsed[1])
        return versions

    def files(self, package, version):
        page = urljoin(self.index, quote(package) + "/")
        parser = Links()
        parser.feed(self.read(page, missing=True).decode())
        files = {}
        for link in parser.urls:
            url = urljoin(page, link)
            name = unquote(Path(urlsplit(url).path).name)
            try:
                parsed = (
                    parse_wheel_filename(name)
                    if name.endswith(".whl")
                    else parse_sdist_filename(name)
                )
            except ValueError:
                continue
            if parsed[0] == package and parsed[1] == Version(version):
                clean = urlunsplit((*urlsplit(url)[:4], ""))
                files[name] = hashlib.sha256(self.read(clean)).hexdigest()
        return files


class PublicRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        PublicPyPI.validate_url(newurl)
        return super().redirect_request(request, fp, code, msg, headers, newurl)


class PublicPyPI(Registry):
    """Read public package bytes without loading any registry credentials."""

    def __init__(self):
        self.index = "https://pypi.org/simple/"

    @staticmethod
    def validate_url(url):
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"pypi.org", "files.pythonhosted.org"}
            or parsed.username
            or parsed.password
            or parsed.port not in {None, 443}
        ):
            raise CheckError("Public PyPI artifact points to an unexpected host.")

    def read(self, url, missing=False):
        self.validate_url(url)
        try:
            with build_opener(PublicRedirects()).open(
                Request(url), timeout=60
            ) as response:
                return response.read()
        except HTTPError as error:
            if missing and error.code == 404:
                return b""
        except (URLError, OSError):
            pass
        raise CheckError(
            "Public PyPI read failed; publication status is unknown."
        ) from None


class Registries:
    """Use both registries for allocation and every completion boundary."""

    def __init__(self, index):
        self.qlty = Registry(index)
        self.pypi = PublicPyPI()

    def versions(self, package):
        return self.qlty.versions(package) | self.pypi.versions(package)

    def files(self, package, version):
        qlty = self.qlty.files(package, version)
        pypi = self.pypi.files(package, version)
        if any(name in qlty and qlty[name] != value for name, value in pypi.items()):
            raise CheckError(
                "Registry contents disagree; retain originals and reconcile."
            )
        return qlty | pypi


def project():
    metadata = tomllib.loads(Path("pyproject.toml").read_text())
    indexes = [
        index for index in metadata["tool"]["uv"]["index"] if index["name"] == "qlty"
    ]
    if len(indexes) != 1:
        raise CheckError("Configure exactly one uv index named qlty.")
    Registry(indexes[0]["url"])
    if (
        indexes[0].get("publish-url", "").rstrip("/")
        != "https://europe-north1-python.pkg.dev/fyndiq/qlty"
    ):
        raise CheckError("Configure the infrastructure-managed qlty publish-url.")
    return metadata, indexes[0]["url"]


def remote(ref):
    lines = git_output("ls-remote", "origin", ref).splitlines()
    if not lines:
        return ""
    if len(lines) != 1:
        raise CheckError("Ambiguous remote ref.")
    return lines[0].split()[0]


def verify_legacy_bootstrap(github, registries, package, version, tag, releases):
    if (github.repository, package, version, tag) != (
        "fyndiq/confluent_kafka_helpers",
        "confluent-kafka-helpers",
        "1.6.0",
        "v1.6.0",
    ):
        return False
    expected = {
        "confluent_kafka_helpers-1.6.0-py3-none-any.whl": "c9dce8d6f4647f702b7b1e0c6acd85f7e0c912ed901daca5ba3de82729e81a06"
    }
    if (
        git_output("rev-parse", f"refs/tags/{tag}^{{commit}}")
        != "c28b13b173a0f01c668cd0f27383df34c64c6cd8"
        or registries.pypi.files(package, version) != expected
        or any(r["tag_name"] == tag and r["draft"] for r in releases)
    ):
        raise CheckError(
            "Legacy baseline differs from the pinned public wheel and commit."
        )
    return True


def verify_baseline(github, registries, package, version, tag, releases):
    if verify_legacy_bootstrap(github, registries, package, version, tag, releases):
        return
    original = None
    matches = [r for r in releases if r["tag_name"] == tag]
    if any(r["draft"] for r in matches):
        raise CheckError("Stable baseline still has a pending GitHub Release.")
    if matches:
        originals = [
            a for a in github.assets(matches[0]) if a["name"] == "manifest.json"
        ]
        if originals:
            try:
                original = retained_candidate(github, matches[0])
            except CheckError:
                raise CheckError(
                    "Completed stable baseline differs from its published originals."
                ) from None
            if (
                original.version != version
                or original.package != package
                or not verify_existing_tag(original)
            ):
                raise CheckError(
                    "Completed stable baseline differs from its published originals."
                )
    if original is None and git_output(
        "for-each-ref", "--format=%(contents)", f"refs/tags/{tag}"
    ).startswith(MARKER + "\n"):
        raise CheckError(
            "Automation baseline is missing its original publication manifest."
        )
    for registry in (registries.qlty, registries.pypi):
        found = registry.files(package, version)
        if original is not None:
            if original.artifacts != found:
                raise CheckError(
                    "Completed stable baseline differs from its published originals."
                )
        elif not any(name.endswith(".whl") for name in found) or not any(
            name.endswith(".tar.gz") for name in found
        ):
            raise CheckError("Stable baseline publication is incomplete or unverified.")


def metadata_tag_rules(metadata):
    return TagRules.from_settings(
        defaults.DEFAULT_SETTINGS | metadata["tool"]["commitizen"]
    )


def metadata_baseline(metadata):
    rules = metadata_tag_rules(metadata)
    tag = rules.find_tag_for(git.get_tags(), metadata["project"]["version"])
    return tag.name if tag else rules.normalize_tag(metadata["project"]["version"])


def retained_candidate(github, release):
    if release.get("draft", True):
        return draft_candidate(release, github.repository)
    assets = [a for a in github.assets(release) if a["name"] == "manifest.json"]
    if len(assets) != 1:
        raise CheckError(
            "Required originals are missing; restore the original manifest."
        )
    candidate = Candidate.parse(github.download(assets[0]), github.repository)
    if candidate.tag != release["tag_name"]:
        raise CheckError("Release tag differs from retained identity.")
    candidate.verify("notes.md", (release.get("body") or "").encode())
    return candidate


def reconcile_registries(registries, candidate, complete=False):
    qlty = reconcile_registry(registries.qlty, candidate, complete, label="qlty")
    pypi = reconcile_registry(registries.pypi, candidate, complete, label="PyPI")
    return qlty | pypi


def reconcile_registry(registry, candidate, complete=False, *, label="Registry"):
    found = registry.files(candidate.package, candidate.version)
    if any(
        name not in candidate.artifacts or candidate.artifacts[name] != checksum
        for name, checksum in found.items()
    ):
        raise CheckError(f"{label} content differs from the retained originals.")
    if complete and found != candidate.artifacts:
        missing = ", ".join(sorted(candidate.artifacts.keys() - found.keys()))
        raise CheckError(
            f"{label} publication is incomplete; missing expected files: {missing}; "
            "resume the same tag."
        )
    return found


def wait_for_registry(registry, candidate, label):
    """Wait for missing originals after a successful upload, without retrying writes."""
    started = monotonic()
    deadline = started + 120
    delay = 2
    while True:
        found = reconcile_registry(registry, candidate, label=label)
        if found == candidate.artifacts:
            return
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        sleep(min(delay, remaining))
        delay = min(delay * 2, 30)
        if monotonic() >= deadline:
            break
    missing = ", ".join(sorted(candidate.artifacts.keys() - found.keys()))
    raise CheckError(
        f"{label} publication is incomplete after {monotonic() - started:.1f}s; "
        f"missing expected files: {missing}; resume the same tag."
    )


def verify_existing_tag(candidate):
    if not remote(f"refs/tags/{candidate.tag}"):
        return False
    if remote(f"refs/tags/{candidate.tag}^{{}}") != candidate.release_sha:
        raise CheckError("Existing tag is not the original annotated release tag.")
    git_output(
        "fetch", "origin", f"refs/tags/{candidate.tag}:refs/tags/{candidate.tag}"
    )
    message = git_output(
        "for-each-ref", "--format=%(contents)", f"refs/tags/{candidate.tag}"
    )
    if message != candidate.tag_message:
        raise CheckError("Tag pending metadata differs from the original manifest.")
    return True
