"""Announce verified stable releases, retaining append-only delivery receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import requests
from library_ci import CheckError
from release_context import (
    GitHub,
    Registries,
    retained_candidate,
    verify_existing_tag,
    wait_for_registry,
)

SCHEMA = "library-slack-v1"
PREFIX = "notification-state-"
PLAN = "notification-plan.json"
MAX_PART_LENGTH = 3000  # Persisted v1 contract, independent of the rendering target.
LIMIT = MAX_PART_LENGTH


def escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def markdown(text):
    def prose(part):
        part = escape(part)
        part = re.sub(r"(?m)^#{1,6} (.+)$", r"*\1*", part)
        return re.sub(r"\*\*([^\n]+?)\*\*", r"*\1*", part)

    # Isolate code and links before formatting prose, preserving destinations.
    token = re.compile(
        r"```[\s\S]*?(?:```|$)|`[^`\n]*`|\[([^\]\n]+)\]\((https?://[^\s)<>|]+)\)"
    )
    sentinel = "\0"
    while sentinel in text:
        sentinel += "\0"
    protected = []

    def protect(match):
        if match[1] is None:
            rendered = escape(match[0])
        else:
            label, url = prose(match[1]).replace("|", "¦"), escape(match[2])
            link = f"<{url}|{label}>"
            rendered = link if len(link) < LIMIT - 8 else f"{label} ({url})"
        protected.append(rendered)
        return f"{sentinel}{len(protected) - 1}{sentinel}"

    formatted = prose(token.sub(protect, text))
    return re.sub(
        re.escape(sentinel) + r"(\d+)" + re.escape(sentinel),
        lambda match: protected[int(match[1])],
        formatted,
    )


def chunks(text):
    result = []
    fenced = False
    while text:
        prefix = "```\n" if fenced else ""
        budget = LIMIT - len(prefix) - 4
        end = min(len(text), budget)
        if end < len(text):
            newline = text.rfind("\n", 0, end + 1)
            if newline > 0:
                end = newline
            # Protect generated links, entities and fence delimiters.
            for pattern in (r"<[^>]*>", r"&(?:amp|lt|gt);", r"`{1,3}"):
                for match in re.finditer(pattern, text):
                    if match.start() < end < match.end():
                        end = match.start()
                        break
        if end == 0:
            raise CheckError("Cannot safely split release notes.")
        part, text = text[:end], text[end:]
        fenced ^= part.count("```") % 2 == 1
        result.append(prefix + part + ("\n```" if fenced else ""))
    return result


class Rejected(CheckError):
    """Slack explicitly rejected this write without accepting a message."""


class Slack:
    def __init__(self, token):
        self.token = token

    def request(self, method, **data):
        for attempt in range(4):
            try:
                response = requests.post(
                    f"https://slack.com/api/{method}",
                    headers={"Authorization": f"Bearer {self.token}"},
                    json=data,
                    timeout=30,
                )
                if response.status_code == 429:
                    delay = int(response.headers.get("Retry-After", "1"))
                    if attempt < 3 and 0 <= delay <= 60:
                        time.sleep(delay)
                        continue
                    raise Rejected(
                        "Slack rate limit exhausted; retry this announcement."
                    )
                if response.status_code != 200:
                    raise CheckError(
                        "Slack transport failed; reconcile the pending send."
                    )
                result = response.json()
            except (requests.RequestException, ValueError):
                raise CheckError(
                    "Slack response unavailable; reconcile the pending send."
                ) from None
            if not result.get("ok"):
                error = result.get("error")
                if error in {"ratelimited", "rate_limited"} and attempt < 3:
                    time.sleep(1)
                    continue
                if error in {
                    "not_authed",
                    "invalid_auth",
                    "token_revoked",
                    "account_inactive",
                    "missing_scope",
                    "no_permission",
                    "channel_not_found",
                    "not_in_channel",
                    "is_archived",
                    "msg_too_long",
                    "invalid_arguments",
                    "invalid_metadata",
                    "ratelimited",
                    "rate_limited",
                }:
                    raise Rejected(
                        "Slack rejected the request; check bot access and retry."
                    )
                raise CheckError(
                    "Slack acceptance is unknown; reconcile the pending send."
                )
            return result
        raise AssertionError("unreachable")

    def find(self, channel, root, identity, index, bot, text, *, oldest):
        cursor = ""
        found = []
        while True:
            arguments = {
                "channel": channel,
                "limit": 100,
                "include_all_metadata": True,
                "cursor": cursor,
            }
            if root:
                arguments["ts"] = root
            else:
                arguments.update(oldest=oldest, inclusive=True)
            result = self.request(
                "conversations.replies" if root else "conversations.history",
                **arguments,
            )
            for message in result["messages"]:
                payload = message.get("metadata", {}).get("event_payload", {})
                if (
                    message.get("user") == bot
                    and payload
                    == {
                        "release": identity,
                        "chunk": str(index),
                        "sha256": hashlib.sha256(text.encode()).hexdigest(),
                    }
                    and message.get("metadata", {}).get("event_type") == SCHEMA
                    and (
                        message.get("thread_ts") == root
                        if root
                        else message.get("thread_ts") in {None, message.get("ts")}
                    )
                ):
                    found.append(message["ts"])
            cursor = result.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                if result.get("has_more"):
                    raise CheckError("Slack reconciliation pagination is incomplete.")
                break
        if len(found) != 1:
            raise CheckError(
                "Pending Slack send is absent or ambiguous; resolve it before retrying."
            )
        return found[0]


class Receipts:
    def __init__(self, github, release, identity, channel, render):
        self.github, self.release = github, release
        assets = github.assets(release)
        plans = [a for a in assets if a["name"] == PLAN]
        assets = sorted(
            (a for a in assets if a["name"].startswith(PREFIX)),
            key=lambda a: a["name"],
        )
        identity_fields = {"schema": SCHEMA, "release": identity, "channel": channel}
        if len(plans) > 1 or (assets and not plans):
            raise CheckError(
                "Notification plan is missing or ambiguous; restore originals."
            )
        if plans:
            plan = json.loads(github.download(plans[0]))
        else:
            parts = render()
            plan = dict(
                identity_fields,
                parts=parts,
                content=hashlib.sha256(json.dumps(parts).encode()).hexdigest(),
            )
        parts = plan.get("parts")
        if (
            any(plan.get(k) != v for k, v in identity_fields.items())
            or not isinstance(parts, list)
            or not parts
            or not all(
                isinstance(p, str) and 0 < len(p) <= MAX_PART_LENGTH for p in parts
            )
            or plan.get("content")
            != hashlib.sha256(json.dumps(parts).encode()).hexdigest()
        ):
            raise CheckError(
                "Notification plan identity or content is invalid; restore originals."
            )
        if not plans:
            self.retain(PLAN, plan)
        self.parts = parts
        count = len(parts)
        self.base = dict(identity_fields, count=count, content=plan["content"])
        self.state = dict(self.base, messages=[], pending=False)
        self.sequence = 0
        for number, asset in enumerate(assets, 1):
            if asset["name"] != f"{PREFIX}{number:06d}.json":
                raise CheckError(
                    "Notification receipt sequence is incomplete; restore originals."
                )
            state = json.loads(github.download(asset))
            if (
                any(state.get(k) != v for k, v in self.base.items())
                or not isinstance(state.get("pending"), bool)
                or not isinstance(state.get("messages"), list)
                or not all(
                    isinstance(ts, str) and re.fullmatch(r"[0-9]+\.[0-9]+", ts)
                    for ts in state["messages"]
                )
                or len(state["messages"]) > count
            ):
                raise CheckError(
                    "Notification receipt identity is invalid; restore originals."
                )
            old, new = self.state, state
            valid = (
                new["messages"] == old["messages"]
                and new["pending"] != old["pending"]
                or old["pending"]
                and not new["pending"]
                and len(new["messages"]) == len(old["messages"]) + 1
                and new["messages"][:-1] == old["messages"]
            )
            if not valid:
                raise CheckError("Notification receipt transition is invalid.")
            self.state, self.sequence = state, number

    def save(self, messages, pending):
        state = dict(self.base, messages=messages, pending=pending)
        self.retain(f"{PREFIX}{self.sequence + 1:06d}.json", state)
        self.state, self.sequence = state, self.sequence + 1

    def retain(self, name, state):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / name
            data = (json.dumps(state, sort_keys=True) + "\n").encode()
            path.write_bytes(data)
            self.github.upload(self.release, path)
            assets = [a for a in self.github.assets(self.release) if a["name"] == name]
            if len(assets) != 1 or self.github.download(assets[0]) != data:
                raise CheckError(
                    "Notification state was not retained; retry the announcement."
                )


def announce(github, tag, channel, slack, registry_factory=None):
    if not re.fullmatch(r"C[A-Z0-9]+", channel):
        raise CheckError("Configure a Slack channel ID.")
    releases = [r for r in github.releases() if r["tag_name"] == tag]
    if len(releases) != 1 or releases[0]["draft"]:
        raise CheckError("Announcement requires one completed GitHub Release.")
    release = releases[0]
    candidate = retained_candidate(github, release)
    if candidate.prerelease or release.get("prerelease"):
        return
    if json.loads(candidate.original).get("notification") != SCHEMA:
        return
    try:
        published = datetime.fromisoformat(release.get("published_at", ""))
        if published.utcoffset() is None or published.timestamp() <= 0:
            raise ValueError
        oldest = f"{published.timestamp():.6f}"
    except (TypeError, ValueError, OverflowError, OSError):
        raise CheckError(
            "GitHub Release publication timestamp is missing or invalid."
        ) from None
    if not verify_existing_tag(candidate):
        raise CheckError("Announcement requires the original annotated release tag.")
    registries = (registry_factory or Registries)(candidate.index)
    wait_for_registry(registries.qlty, candidate, "qlty")
    wait_for_registry(registries.pypi, candidate, "PyPI")
    assets = [a for a in github.assets(release) if a["name"] == "notes.md"]
    if len(assets) != 1:
        raise CheckError("Restore canonical notes.md before announcing.")
    notes = github.download(assets[0])
    candidate.verify("notes.md", notes)
    if release.get("body") != notes.decode():
        raise CheckError("Published body differs from canonical notes.md.")
    url = release["html_url"]
    expected = f"https://github.com/{github.repository}/releases/"
    if not url.startswith(expected) or any(c in url for c in "<>|\n"):
        raise CheckError("Invalid GitHub Release link.")

    def render():
        return chunks(
            f"*{escape(candidate.package)} {escape(candidate.version)}*\n<{url}|GitHub Release>\n\n"
            + markdown(notes.decode())
        )

    identity = hashlib.sha256(candidate.original).hexdigest()
    receipts = Receipts(github, release, identity, channel, render)
    parts = receipts.parts
    bot = slack.request("auth.test")["user_id"]
    while len(receipts.state["messages"]) < len(parts):
        messages = receipts.state["messages"]
        index = len(messages)
        if receipts.state["pending"]:
            ts = slack.find(
                channel,
                messages[0] if messages else "",
                identity,
                index,
                bot,
                parts[index],
                oldest=oldest,
            )
            receipts.save([*messages, ts], False)
            continue
        receipts.save(messages, True)
        arguments = {
            "channel": channel,
            "text": parts[index],
            "unfurl_links": False,
            "unfurl_media": False,
            "parse": "none",
            "link_names": False,
            "metadata": {
                "event_type": SCHEMA,
                "event_payload": {
                    "release": identity,
                    "chunk": str(index),
                    "sha256": hashlib.sha256(parts[index].encode()).hexdigest(),
                },
            },
        }
        if messages:
            arguments["thread_ts"] = messages[0]
        try:
            result = slack.request("chat.postMessage", **arguments)
        except Rejected:
            receipts.save(messages, False)
            raise
        ts = result.get("ts", "")
        if not re.fullmatch(r"[0-9]+\.[0-9]+", ts):
            raise CheckError(
                "Slack message identity unavailable; reconcile the pending send."
            )
        receipts.save([*messages, ts], False)


def from_environment(github, tag, channel):
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not token:
        raise CheckError("Configure the existing Deploys bot token for announcements.")
    announce(github, tag, channel, Slack(token))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--channel", default="C0CE6LY0T0C")
    parser.add_argument("--working-directory", default=".")
    args = parser.parse_args()
    try:
        os.chdir(args.working_directory)
        from_environment(GitHub(args.repository), args.tag, args.channel)
    except CheckError as error:
        print(f"Library announcement failed: {error}", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001 - backend errors can contain credentials
        print(
            "Library announcement failed: invalid metadata or unavailable tooling. Retry the same tag after correcting the cause.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
