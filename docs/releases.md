# Releases

Merges to `master` run the local release workflow. Commitizen calculates the next
stable version from Conventional Commits. The workflow checks the selected source,
builds one wheel and one sdist on Python 3.10, and retains those original bytes,
notes, manifest, and prepared Git bundle before publication. Release automation
runs on Python 3.12 with uv 0.13.1 and Commitizen 4.13.9.

The publisher pushes the prepared stable version commit and annotated tag together.
It uploads the originals to qlty first, then uploads the same files to PyPI. Both
registries must serve the exact original SHA256 hashes before the GitHub Release
can complete. A stable release then posts the retained notes to Slack. Prereleases
publish to both registries without moving `master` or posting to Slack.

## Setup

Provision the repository's GitHub resources first: the `qlty-publish` environment
with `RELEASE_APP_ID` and `RELEASE_APP_KEY`, and repository-scoped
`PYPI_API_TOKEN` and `SLACK_DEPLOYS_BOT_TOKEN` secrets. The GitHub App needs
repository contents write access and the protected branch bypass used for release
commits. The PyPI token must permit publishing `confluent-kafka-helpers`.
After applying the GitHub producer changes, re-plan and apply the dependent IAM
changes that grant this repository workload identity access to
`gha-qlty-publish@fyndiq.iam.gserviceaccount.com`.

The App token performs Git and Release writes. The workflow token reads and
retains Actions artifacts and dispatches source-race retries. Complete credentials,
IAM access, and baseline bootstrap before enabling the release workflow and
required release-preview checks.

Before enabling release checks, a maintainer must create and push `v1.6.0` at
`c28b13b173a0f01c668cd0f27383df34c64c6cd8`. Verify the target commit before pushing.
The narrowly scoped bootstrap accepts only this repository, version, tag, and
commit, together with the existing public wheel
`confluent_kafka_helpers-1.6.0-py3-none-any.whl` and SHA256
`c9dce8d6f4647f702b7b1e0c6acd85f7e0c912ed901daca5ba3de82729e81a06`.
That legacy release has no sdist and does not require a qlty backfill. Every new
release requires exactly one wheel and one sdist in both registries.

## Installation

Create or activate a virtual environment before installing. From public PyPI:

```sh
uv venv
uv pip install --index https://pypi.org/simple confluent-kafka-helpers
```

For qlty, configure Google Application Default Credentials with access to its
Artifact Registry repository, then install the Google keyring helper and package:

```sh
uv tool install --with keyrings.google-artifactregistry-auth==1.1.2 keyring==25.7.0
uv pip install --index https://oauth2accesstoken@europe-north1-python.pkg.dev/fyndiq/qlty/simple/ --keyring-provider subprocess confluent-kafka-helpers
```

Both registries serve the same distribution bytes for each new release. Pin a
version with `confluent-kafka-helpers==<version>` when reproducibility is required.
To install a prerelease, specify its exact version in that requirement.

## Manual releases and recovery

Run the Release workflow on `master`. Leave all inputs empty for a stable release.
For a prerelease, supply all three inputs: `source_ref` (branch, tag, or SHA),
`target_version` (stable `major.minor.patch`), and `phase` (`alpha`, `beta`, or `rc`).
The workflow fixes the source SHA and allocates the next unused sequence across
Git tags and both registries. It preserves that allocation across reruns.

After preparation or artifact-upload failures, rerun all jobs so inspection can
select the original retained candidate. Failed or missing preparation evidence
requires restoring originals before another preparation attempt.

For interrupted publication, dispatch with `resume_tag` alone. The workflow uses
the retained originals and version, verifies existing files in each registry, and
continues missing uploads. A partial upload, hash disagreement, missing original,
or lost publication evidence blocks newer releases. Do not rebuild distributions,
replace retained assets, delete published files, move tags, or choose another
version to bypass a failed release. Restore missing originals or correct access,
then resume the same tag. Actions transport artifacts expire after 90 days;
the draft GitHub Release retains the canonical originals for recovery.

For a failed Slack announcement after a completed stable release, dispatch with
`announce_tag` alone. The workflow verifies the tag, retained notes, both registry
publications, and Slack history before posting. A retry can reconcile a successful
post whose response was lost. Leave `release_retry` at `0`; source-race retries are
bounded and managed by automation.

Source checks and builds have no publishing secrets. The publisher executes the
orchestrator from the trusted workflow snapshot and reads selected-source
metadata as data; it never executes the selected source or its build backend.
PyPI reads use only HTTPS public PyPI hosts, without credentials, and hash the
actual downloaded bytes. Only the PyPI upload child receives the PyPI token.
