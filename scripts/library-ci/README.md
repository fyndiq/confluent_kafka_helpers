# Library CI

`library_ci.py` validates every PR-introduced complete commit message with
Commitizen's Conventional Commits schema, builds a wheel and source distribution
with the project's build backend, and calculates a read-only next-version preview.
The reusable workflow and composite action supply the pinned tools. No publishing
credentials, API tokens, notifications, Git writes, or version updates are needed
for this validation.

Vendored from `fyndiq/.github` at `247d8aacd52026d43e18d9cefd1b1097b3bc900c`.
Local extensions publish the same retained bytes to qlty and public PyPI.
For setup, releases, and recovery, see [Releases](../../docs/releases.md).

Run the tests from the repository root:

```sh
uv run --no-project --no-config --python 3.12 --with commitizen==4.13.9 --with google-auth==2.40.3 --with requests==2.32.5 python -m unittest discover -s scripts/library-ci -v
uvx ruff check --target-version py312 scripts/library-ci
```

Tests create isolated temporary Git histories and exercise real Commitizen
previews, including complete message validation, historical merges, breaking
changes, pre-1.0 policy, stable baselines, and expected/unexpected failures.

To run a local preview and build from a clean library checkout:

```sh
uv run --no-project --no-config --python 3.12 --with commitizen==4.13.9 \
  python scripts/library-ci/library_ci.py \
  --release-paths "package_name/*
pyproject.toml
README.md" --base <pr-base-sha> --head HEAD
```

Build outputs are temporary and removed after validation. Failures report their
stage and exit code without printing captured backend or registry diagnostics.
Reproduce a failing build locally with `uv build` to inspect its diagnostics in
the appropriate environment.

## Release-note preparation

`release_notes.py` is a separate preparation entry point. Ordinary CI keeps its
read-only, token-free preview. From a clean library checkout with complete tags:

```sh
uv run --no-project --no-config --python 3.12 --with commitizen==4.13.9 \
  python scripts/library-ci/release_notes.py \
  --release-paths "package_name/*
pyproject.toml
README.md" --repository fyndiq/library-name --output /tmp/new-release.md
```

Supply `GH_TOKEN` through the environment with read access to the repository and
its pull requests. `--repository` defaults to `GITHUB_REPOSITORY`. The output path
must be outside the checkout and must not already exist. Preparation changes no
library files, refs, or versions. No increment is an explicit preparation failure;
the release caller should first use the normal preview to decide whether to run.

The adapter uses Commitizen 4.13.9 for parsing, version calculation, grouping, tag
format, and the stock Markdown template. It adds full Markdown PR links through a
message hook. Subject-only `!` markers stay visible as `BREAKING CHANGE` text;
body breaking notes and multiline descriptions remain intact. Caller templates
cannot override the stock template and remove links.

For each changelog SHA, the adapter queries GitHub's commit-to-PR endpoint and
caches the result for this preparation. It selects merged PRs targeting this
repository, preferring an exact merge SHA match; multiple remaining PRs fail as
ambiguous. Successful responses without a corresponding merged PR use commit
links. Permission, network, malformed-response, and pagination failures abort
before writing output. PR numbers are never inferred from commit text. Deduplication
compares complete entries within their group, including scope, description,
breaking marker, and link. Distinct changes from one PR stay visible.

The UTF-8 output is the canonical new-release section, including its version/date
heading and trailing newline. It covers only the stable baseline through HEAD;
intermediate prerelease tags do not split it, and historical CHANGELOG sections
are never copied into release notes. The release publisher retains this file
with its release artifacts and uses its exact contents as the published GitHub
Release body. To update CHANGELOG, import `updated_changelog(section, existing)`
from this module and write the returned text during release preparation. That
helper uses Commitizen's incremental insertion and preserves the exact section.
Publication verification and recovery remain the release workflow's responsibility;
a local baseline tag alone does not verify publication.

The tests mock GitHub and cover rebase commits, cached and missing associations,
pagination and failures, identical and distinct entries, breaking notes, pre-1.0
policy, historical merges, canonical insertion, and output integrity. They do not
require credentials or make live API calls.

## Release implementation

`library_release.py` supplies inspect, prepare, publish, and preparation-receipt
commands for the local `library-release` action. `release_plan.py` selects
preparation, reuse, or recovery with an immutable source and allocation.
`release_recovery.py` validates prior attempts and transports retained originals.
`release_context.py` reads GitHub and both registries; `release_notifications.py`
verifies completed stable releases and retains Slack delivery receipts.

Each candidate retains exactly these files:

- `manifest.json`: schema `library-release-v1`, repository, normalized package,
  version, tag, source/release SHAs, baseline tag/version, qlty index, distribution
  SHA256 hashes, retained-file hashes, and producing Actions run ID and attempt.
- `notes.md`: canonical UTF-8 notes, inserted into the stable changelog and used
  verbatim as the published GitHub Release body.
- `release.bundle`: the prepared commit relative to the tested stable source, or
  source history above the stable baseline for a prerelease.
- One wheel and one `.tar.gz` source distribution.

The annotated tag message is `library-release-v1`, a newline, and the SHA256 of
exact manifest bytes. A draft initially retains the manifest in a hidden body
comment so interrupted asset uploads can recover their original identity.
Completion removes the comment. Candidate validation, original-hash checks,
registry reconciliation, and tag verification are shared by staging, publication,
recovery, and announcements. Both registries are checked independently; merging
partial file sets never establishes completion.

After each successful upload, the publisher retries verification of missing files
with backoff for up to 120 seconds. Registry reads already in progress can extend
the elapsed time beyond that window. A timeout identifies the registry, missing
expected filenames, and elapsed wait. Hash conflicts, unexpected files, and read
errors fail immediately. Resume the same tag after correcting the cause.

Source-race cleanup requires an absent remote tag and no publication files in
either registry. Stable Git publication uses an atomic commit/tag push with an
expected-head lease. Prereleases preserve `master` and validate that only the
project version and lock entry changed in the prepared snapshot.

GitHub Release assets retain canonical originals. Actions artifacts provide
90-day transport: `library-candidate-<attempt>`, `library-allocation`, and
`library-preparation-<attempt>`. Whole-run retries reuse the original allocation
and candidate. Without a candidate, earlier preparation receipts must prove that
no version was prepared. Missing or failed evidence requires restoring originals.
Only missing assets allow fallback to the exact producing run and attempt;
corrupt assets fail without fallback or replacement.

Slack uses append-only notification plan/state assets and deterministic message
identities. A pending receipt records intent before each post. Recovery checks
Slack history before retrying a post with an uncertain result.
