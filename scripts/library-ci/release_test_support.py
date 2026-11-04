"""Reusable temporary Git histories and stateful publication fakes."""

import copy
import json
import os
import subprocess
import tarfile
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import library_release as release
from release_context import Registries


class GitHub:
    repository = "fyndiq/example"
    token = "test-only"

    def __init__(self):
        self.records = []
        self.data = {}
        self.operations = []
        self.fail_upload = ""
        self.fail_complete = False

    def releases(self):
        return copy.deepcopy(self.records)

    def assets(self, item):
        return [
            {"name": name, "url": f"asset/{item['id']}/{name}"}
            for rid, name in self.data
            if rid == item["id"]
        ]

    def download(self, asset):
        _, rid, name = asset["url"].split("/", 2)
        return self.data[int(rid), name]

    def upload(self, item, path):
        self.operations.append(("upload", path.name))
        if path.name == self.fail_upload:
            raise release.CheckError("Injected asset failure")
        self.data[item["id"], path.name] = path.read_bytes()

    def request(self, method, path, data=None, raw=False):
        self.operations.append((method, path))
        if method == "POST" and path == "releases":
            item = dict(
                data,
                id=len(self.records) + 1,
                html_url="https://github.com/fyndiq/example/releases/tag/v1.0.1",
            )
            self.records.append(item)
            return copy.deepcopy(item)
        if method == "PATCH":
            if self.fail_complete:
                raise release.CheckError("Injected GitHub completion failure")
            self.records[-1].update(data)
            if data.get("draft") is False:
                self.records[-1]["published_at"] = "2026-11-03T10:00:00Z"
            return copy.deepcopy(self.records[-1])
        if method == "DELETE":
            self.records.clear()
            return None
        raise AssertionError((method, path))


class Registry:
    def __init__(self):
        self.current = {}
        self.baseline = {
            "example-1.0.0-py3-none-any.whl": "old",
            "example-1.0.0.tar.gz": "old",
        }

    def files(self, package, version):
        return dict(self.baseline if version == "1.0.0" else self.current)


def registries():
    destinations = Registries.__new__(Registries)
    destinations.qlty = Registry()
    destinations.pypi = Registry()
    return destinations


class LifecycleFixture:
    def setUp(self):
        environment = patch.dict(
            os.environ,
            {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"},
        )
        environment.start()
        self.addCleanup(environment.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.old_directory = Path.cwd()
        self.addCleanup(os.chdir, self.old_directory)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        os.chdir(self.repo)
        self.git("init", "--initial-branch=master")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.com")
        Path("source.py").write_text("value = 1\n")
        self.git("add", ".")
        self.git("commit", "-m", "feat: baseline")
        self.git("tag", "v1.0.0")
        Path("source.py").write_text("value = 2\n")
        self.git("commit", "-am", "fix: update")
        self.source = self.git("rev-parse", "HEAD")
        self.origin = self.root / "origin.git"
        self.git("init", "--bare", str(self.origin))
        self.git("remote", "add", "origin", str(self.origin))
        self.git("push", "origin", "master", "refs/tags/v1.0.0")
        Path("version.txt").write_text("1.0.1\n")
        self.git("add", "version.txt")
        self.git("commit", "-m", "chore: release 1.0.1")
        self.sha = self.git("rev-parse", "HEAD")
        self.directory = self.root / "candidate"
        self.directory.mkdir()
        self.git("update-ref", "refs/library-release/candidate", self.sha)
        self.git(
            "bundle",
            "create",
            str(self.directory / "release.bundle"),
            "refs/library-release/candidate",
            "^" + self.source,
        )
        (self.directory / "notes.md").write_text("## 1.0.1\n\n- update\n")
        for name in ("example-1.0.1-py3-none-any.whl", "example-1.0.1.tar.gz"):
            (self.directory / name).write_bytes(name.encode())
        self.manifest = {
            "schema": release.MARKER,
            "repository": GitHub.repository,
            "package": "example",
            "version": "1.0.1",
            "source_sha": self.source,
            "release_sha": self.sha,
            "tag": "v1.0.1",
            "baseline": "v1.0.0",
            "baseline_version": "1.0.0",
            "index": "https://europe-north1-python.pkg.dev/fyndiq/qlty/simple/",
            "transport": {"run_id": "42", "attempt": "1"},
            "artifacts": {
                p.name: release.digest(p)
                for p in self.directory.iterdir()
                if p.name.endswith((".whl", ".tar.gz"))
            },
            "retained": {
                name: release.digest(self.directory / name)
                for name in ("release.bundle", "notes.md")
            },
        }
        self.write_manifest()
        self.github = GitHub()
        self.registries = registries()
        self.registry = self.registries.qlty
        self.pypi = self.registries.pypi
        self.real_subprocess_run = subprocess.run
        self.subprocess_patch = patch.object(
            subprocess, "run", side_effect=self.child_command
        )
        self.subprocess_patch.start()
        self.addCleanup(self.subprocess_patch.stop)
        token_environment = patch.dict(os.environ, {"PYPI_API_TOKEN": "test-only"})
        token_environment.start()
        self.addCleanup(token_environment.stop)
        self.real_run = release.run
        self.fail_publish = False
        self.publish_calls = 0
        self.fail_push = False
        self.run_patch = patch.object(release, "run", side_effect=self.command)
        self.run_patch.start()
        self.addCleanup(self.run_patch.stop)

    def child_command(self, args, **kwargs):
        if args[:2] == ["uv", "publish"]:
            self.pypi.current.update(self.manifest["artifacts"])
            return subprocess.CompletedProcess(args, 0, "", "")
        return self.real_subprocess_run(args, **kwargs)

    def git(self, *args):
        result = subprocess.run(
            ["git", *args], check=True, capture_output=True, text=True
        )
        return result.stdout.strip()

    def write_manifest(self):
        (self.directory / "manifest.json").write_text(
            json.dumps(self.manifest, sort_keys=True) + "\n"
        )

    def command(self, *args):
        if args[:2] == ("uv", "publish"):
            self.publish_calls += 1
            self.assertEqual(
                self.git(
                    "ls-remote", "origin", f"refs/tags/{self.manifest['tag']}^{{}}"
                ).split()[0],
                self.sha,
            )
            self.assertTrue(
                all(
                    (self.github.records[-1]["id"], name) in self.github.data
                    for name in release.load(
                        self.directory, self.github.repository
                    ).hashes
                )
            )
            self.registry.current.update(self.manifest["artifacts"])
            if self.fail_publish:
                self.registry.current.pop(next(iter(self.registry.current)))
                return subprocess.CompletedProcess(args, 1, "", "sensitive diagnostics")
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[:2] == ("git", "push") and self.fail_push:
            return subprocess.CompletedProcess(args, 1, "", "injected")
        return self.real_run(*args)

    def publish(self, resume="", retry=0):
        release.publish(
            self.github,
            self.directory,
            resume,
            "master",
            retry,
            registry_factory=lambda _: self.registries,
            followup_stable=os.environ.get("FOLLOWUP_STABLE") == "true",
        )

    def prerelease_candidate(self, directory="."):
        project = Path(directory)
        project.mkdir(exist_ok=True)
        self.git("reset", "--hard", self.source)
        (project / "pyproject.toml").write_text(
            '[project]\nname = "example"\nversion = "1.0.0"\n[tool.commitizen]\ntag_format = "v$version"\nversion_scheme = "pep440"\n'
        )
        (project / "uv.lock").write_text(
            'version = 1\n[[package]]\nname = "example"\nversion = "1.0.0"\n'
        )
        self.git("add", str(project / "pyproject.toml"), str(project / "uv.lock"))
        self.git("commit", "-m", "chore: configure library")
        self.source = self.git("rev-parse", "HEAD")
        self.git("push", "origin", "HEAD:master")
        self.manifest["source_sha"] = self.source
        (project / "pyproject.toml").write_text(
            (project / "pyproject.toml").read_text().replace("1.0.0", "2.0.0a1")
        )
        (project / "uv.lock").write_text(
            (project / "uv.lock").read_text().replace("1.0.0", "2.0.0a1")
        )
        self.git("add", str(project / "pyproject.toml"), str(project / "uv.lock"))
        self.git("commit", "-m", "chore: release 2.0.0a1")
        self.sha = self.git("rev-parse", "HEAD")
        self.manifest.update(version="2.0.0a1", tag="v2.0.0a1", release_sha=self.sha)
        for name in list(self.manifest["artifacts"]):
            path = self.directory / name
            path.rename(self.directory / name.replace("1.0.1", "2.0.0a1"))
        self.manifest["artifacts"] = {
            p.name: release.digest(p)
            for p in self.directory.iterdir()
            if p.name.endswith((".whl", ".tar.gz"))
        }
        self.git("update-ref", "refs/library-release/candidate", self.sha)
        self.git(
            "bundle",
            "create",
            str(self.directory / "release.bundle"),
            "refs/library-release/candidate",
            "^v1.0.0",
        )
        self.manifest["retained"]["release.bundle"] = release.digest(
            self.directory / "release.bundle"
        )
        self.write_manifest()

    def advance(self):
        self.git("checkout", "--detach", self.source)
        Path("new.txt").write_text("new source\n")
        self.git("add", "new.txt")
        self.git("commit", "-m", "feat: competing merge")
        self.git("push", "origin", "HEAD:master")


class PreparationFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.original = Path.cwd()
        self.addCleanup(os.chdir, self.original)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        os.chdir(self.repo)
        self.environment = patch.dict(
            os.environ,
            {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"},
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.git("init", "-b", "master")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.com")
        Path("pyproject.toml").write_text("""[project]
name = "example"
version = "1.0.0"
[tool.commitizen]
name = "cz_conventional_commits"
version_provider = "uv"
tag_format = "v$version"
version_scheme = "pep440"
pre_bump_hooks = ["touch forbidden-hook"]
[[tool.uv.index]]
name = "qlty"
url = "https://oauth2accesstoken@europe-north1-python.pkg.dev/fyndiq/qlty/simple/"
publish-url = "https://europe-north1-python.pkg.dev/fyndiq/qlty/"
""")
        Path("uv.lock").write_text("""version = 1
[[package]]
name = "example"
version = "1.0.0"
source = { editable = "." }
[[package]]
name = "unrelated"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
""")
        Path("package.py").write_text("value = 1\n")
        self.commit("feat: baseline")
        self.git("tag", "v1.0.0")
        self.baseline = self.git("rev-parse", "HEAD")
        self.github = GitHub()
        self.candidate = self.root / "candidate"

    def git(self, *args):
        return subprocess.run(
            ["git", *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "-m", message)

    def build(self, *args):
        if args[:2] != ("uv", "build"):
            return self.real_checked(*args)
        directory = Path(args[-1])
        version = release.tomllib.loads(Path("pyproject.toml").read_text())["project"][
            "version"
        ]
        with zipfile.ZipFile(
            directory / f"example-{version}-py3-none-any.whl", "w"
        ) as archive:
            archive.writestr(
                f"example-{version}.dist-info/METADATA",
                f"Name: example\nVersion: {version}\n",
            )
        with tarfile.open(directory / f"example-{version}.tar.gz", "w:gz") as archive:
            contents = f"Name: example\nVersion: {version}\n".encode()
            entry = tarfile.TarInfo(f"example-{version}/PKG-INFO")
            entry.size = len(contents)
            archive.addfile(entry, BytesIO(contents))
        return ""


def assert_outputs(output, **expected):
    output.assert_called_once()
    actual = output.call_args.kwargs
    assert all(actual.get(key) == value for key, value in expected.items()), actual


class ArtifactAPI:
    """Stateful Actions store; callers can list/download in any order."""

    def __init__(self, run_id="42"):
        self.run_id = run_id
        self.records = []
        self.data = {}
        self.operations = []

    def add(self, name, files, expired=False):
        identifier = len(self.records) + 1
        self.records.append({"name": name, "id": identifier, "expired": expired})
        archive = BytesIO()
        with zipfile.ZipFile(archive, "w") as zipped:
            for path, data in files.items():
                zipped.writestr(path, data)
        self.data[identifier] = archive.getvalue()
        return self

    def receipt(self, attempt="1", preparation="skipped", inspection="success"):
        return self.add(
            f"library-preparation-{attempt}",
            {
                "preparation.json": json.dumps(
                    {
                        "repository": GitHub.repository,
                        "run_id": self.run_id,
                        "attempt": attempt,
                        "preparation": preparation,
                        "inspection": inspection,
                    }
                )
            },
        )

    def request(self, method, path, data=None, raw=False):
        self.operations.append((method, path))
        if (
            method == "GET"
            and path == f"actions/runs/{self.run_id}/artifacts?per_page=100&page=1"
        ):
            return {"artifacts": copy.deepcopy(self.records)}
        if method == "GET" and raw and path.startswith("actions/artifacts/"):
            return self.data[int(path.split("/")[2])]
        raise AssertionError((method, path))
