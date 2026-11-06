"""Delivery and recovery use stateful stubs, never a live Slack service."""

import copy
import hashlib
import json
import os
import unittest
from unittest.mock import Mock, patch

import requests
import release_context as context
from library_ci import CheckError
from release_notifications import (
    LIMIT,
    PLAN,
    SCHEMA,
    Rejected,
    Slack,
    announce,
    chunks,
    from_environment,
    markdown,
)
from release_test_support import LifecycleFixture
from test_registry_visibility import Clock


class Messages(Slack):
    def __init__(self):
        self.messages = []
        self.fail = ""
        self.calls = []

    def request(self, method, **data):
        self.calls.append((method, data))
        if method == "auth.test":
            return {"user_id": "UBOT"}
        if method.startswith("conversations."):
            messages = self.messages
            if method == "conversations.history":
                messages = [
                    m for m in messages if float(m["ts"]) >= float(data["oldest"])
                ]
            return {"messages": copy.deepcopy(messages)}
        assert method == "chat.postMessage"
        if self.fail == "before":
            self.fail = ""
            raise Rejected("rejected")
        if self.fail == "unknown-before":
            raise CheckError("timeout")
        message = dict(data, ts=f"1793700000.{len(self.messages) + 1}", user="UBOT")
        self.messages.append(message)
        if self.fail == "after":
            self.fail = ""
            raise CheckError("timeout")
        return message


class Delivery(LifecycleFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.manifest["notification"] = SCHEMA
        self.write_manifest()
        self.publish()
        self.slack = Messages()
        self.clock = Clock()
        for name in ("monotonic", "sleep"):
            replacement = patch.object(context, name, getattr(self.clock, name))
            replacement.start()
            self.addCleanup(replacement.stop)

    def announce(self):
        announce(self.github, "v1.0.1", "C123", self.slack, lambda _: self.registries)

    def test_normal_and_repeated_retry(self):
        self.announce()
        for _ in range(3):
            self.announce()
        self.assertEqual(len(self.slack.messages), 1)
        message = self.slack.messages[0]
        self.assertIn("example 1.0.1", message["text"])
        self.assertIn("GitHub Release", message["text"])
        self.assertIn("update", message["text"])
        self.assertFalse(message["unfurl_links"])
        self.assertFalse(message["unfurl_media"])
        self.assertFalse(message["link_names"])
        states = [
            json.loads(data)
            for (rid, name), data in self.github.data.items()
            if name.startswith("notification-state-")
        ]
        self.assertEqual(states[-1]["messages"], [message["ts"]])

    def delayed_visibility(self, destination):
        expected = self.manifest["artifacts"]

        def files(*args):
            self.assertEqual(self.slack.calls, [])
            self.assertNotIn((1, PLAN), self.github.data)
            return next(visible)

        visible = iter([{}, dict(list(expected.items())[:1]), expected])
        with patch.object(destination, "files", side_effect=files) as reads:
            self.announce()
        self.assertEqual(reads.call_count, 3)
        self.assertEqual(self.clock.sleeps, [2, 4])
        self.announce()
        self.assertEqual(len(self.slack.messages), 1)
        self.assertEqual(self.publish_calls, 1)

    def test_announcement_waits_for_qlty_visibility(self):
        self.delayed_visibility(self.registry)

    def test_announcement_waits_for_pypi_visibility(self):
        self.delayed_visibility(self.pypi)

    def test_registry_timeout_never_sends_and_same_tag_retry_sends_once(self):
        for label, destination in (("qlty", self.registry), ("PyPI", self.pypi)):
            with self.subTest(registry=label):
                self.clock.now = 0
                self.clock.sleeps.clear()
                expected = self.manifest["artifacts"]
                visible = dict(list(expected.items())[:1])
                missing = next(iter(expected.keys() - visible.keys()))
                with (
                    patch.object(destination, "files", return_value=visible),
                    self.assertRaises(CheckError) as raised,
                ):
                    self.announce()
                self.assertIn(label, str(raised.exception))
                self.assertIn(missing, str(raised.exception))
                self.assertIn("120.0s", str(raised.exception))
                self.assertEqual(self.clock.now, 120)
                self.assertEqual(self.slack.calls, [])
                self.assertNotIn((1, PLAN), self.github.data)
        self.announce()
        self.announce()
        self.assertEqual(len(self.slack.messages), 1)
        self.assertEqual(self.publish_calls, 1)

    def test_registry_integrity_and_read_failures_never_wait_or_send(self):
        name = next(iter(self.manifest["artifacts"]))
        for destination in (self.registry, self.pypi):
            for result in (
                {name: "changed"},
                {"unexpected.whl": "other"},
                CheckError("Registry read failed"),
                ValueError("invalid metadata"),
            ):
                with self.subTest(registry=destination, result=result):
                    options = (
                        {"side_effect": result}
                        if isinstance(result, Exception)
                        else {"return_value": result}
                    )
                    with patch.object(destination, "files", **options) as reads:
                        with self.assertRaises(
                            type(result) if isinstance(result, Exception) else CheckError
                        ):
                            self.announce()
                    self.assertEqual(reads.call_count, 1)
                    self.assertEqual(self.clock.sleeps, [])
                    self.assertEqual(self.slack.calls, [])
                    self.assertNotIn((1, PLAN), self.github.data)

    def test_rejected_before_send_retries(self):
        self.slack.fail = "before"
        with self.assertRaises(Rejected):
            self.announce()
        self.announce()
        self.assertEqual(len(self.slack.messages), 1)

    def test_ambiguous_after_send_reconciles(self):
        self.slack.fail = "after"
        with self.assertRaises(CheckError):
            self.announce()
        # Slack may normalize text and attach thread_ts after a human replies.
        self.slack.messages[0]["text"] = ":normalized_emoji:"
        self.slack.messages[0]["thread_ts"] = self.slack.messages[0]["ts"]
        self.announce()
        self.announce()
        self.assertEqual(len(self.slack.messages), 1)
        history = [
            data
            for method, data in self.slack.calls
            if method == "conversations.history"
        ]
        self.assertEqual(history[0]["oldest"], "1793700000.000000")
        self.assertTrue(history[0]["inclusive"])

    def test_publication_timestamp_with_offset_and_fraction(self):
        self.github.records[0]["published_at"] = "2026-11-03T11:00:00.100000+01:00"
        self.slack.fail = "after"
        with self.assertRaises(CheckError):
            self.announce()
        self.announce()
        history = [
            data
            for method, data in self.slack.calls
            if method == "conversations.history"
        ]
        self.assertEqual(history[0]["oldest"], "1793700000.100000")
        self.assertEqual(len(self.slack.messages), 1)

    def test_invalid_publication_timestamp_fails_before_notification(self):
        for value in (
            None,
            "",
            "invalid",
            "2026-11-03",
            "2026-11-03T10:00:00",
            42,
            "1970-01-01T00:00:00Z",
        ):
            with self.subTest(value=value):
                self.github.records[0]["published_at"] = value
                with self.assertRaisesRegex(CheckError, "publication timestamp"):
                    self.announce()
                self.assertEqual(self.slack.calls, [])
                self.assertNotIn((1, PLAN), self.github.data)
        self.github.records[0].pop("published_at")
        with self.assertRaisesRegex(CheckError, "publication timestamp"):
            self.announce()

    def test_ambiguous_before_send_fails_closed(self):
        self.slack.fail = "unknown-before"
        with self.assertRaises(CheckError):
            self.announce()
        self.slack.fail = ""
        for _ in range(2):
            with self.assertRaisesRegex(CheckError, "absent or ambiguous"):
                self.announce()
        self.assertEqual(self.slack.messages, [])

    def test_state_save_failure_after_send(self):
        self.github.fail_upload = "notification-state-000002.json"
        with self.assertRaises(CheckError):
            self.announce()
        self.github.fail_upload = ""
        self.announce()
        self.assertEqual(len(self.slack.messages), 1)

    def test_state_save_failure_before_send(self):
        self.github.fail_upload = "notification-state-000001.json"
        with self.assertRaises(CheckError):
            self.announce()
        self.assertEqual(self.slack.messages, [])
        self.github.fail_upload = ""
        self.announce()

    def test_receipt_upload_accepted_but_response_lost(self):
        upload = self.github.upload

        def accepted(item, path):
            upload(item, path)
            if path.name == "notification-state-000002.json":
                raise CheckError("lost response")

        with (
            patch.object(self.github, "upload", side_effect=accepted),
            self.assertRaises(CheckError),
        ):
            self.announce()
        self.announce()
        self.assertEqual(len(self.slack.messages), 1)

    def test_plan_is_retained_before_any_slack_call(self):
        self.github.fail_upload = PLAN
        with self.assertRaises(CheckError):
            self.announce()
        self.assertEqual(self.slack.calls, [])

    def test_plan_upload_response_lost_reuses_original(self):
        upload = self.github.upload

        def accepted(item, path):
            upload(item, path)
            if path.name == PLAN:
                raise CheckError("lost response")

        with (
            patch.object(self.github, "upload", side_effect=accepted),
            self.assertRaises(CheckError),
        ):
            self.announce()
        with patch(
            "release_notifications.markdown",
            side_effect=AssertionError("rendered again"),
        ):
            self.announce()
        self.assertEqual(len(self.slack.messages), 1)

    def test_plan_validation_rejects_missing_changed_and_wrong_identity(self):
        self.announce()
        original = self.github.data[1, PLAN]
        for field, value in [
            ("release", "other"),
            ("channel", "COTHER"),
            ("schema", "other"),
            ("parts", ["changed"]),
            ("parts", ["x" * (LIMIT + 1)]),
        ]:
            with self.subTest(
                field=field, value=value[:20] if isinstance(value, str) else "parts"
            ):
                plan = json.loads(original)
                plan[field] = value
                self.github.data[1, PLAN] = json.dumps(plan).encode()
                with self.assertRaisesRegex(CheckError, "plan identity or content"):
                    self.announce()
        self.github.data.pop((1, PLAN))
        with (
            patch(
                "release_notifications.markdown",
                side_effect=AssertionError("rendered again"),
            ),
            self.assertRaisesRegex(CheckError, "plan is missing"),
        ):
            self.announce()
        self.assertEqual(len(self.slack.messages), 1)

    def test_prerelease_never_sends(self):
        self.github.records[0]["prerelease"] = True
        self.announce()
        self.assertEqual(self.slack.calls, [])

    def test_registry_failure_and_draft_never_send(self):
        self.registry.current = {}
        with self.assertRaises(CheckError):
            self.announce()
        self.github.records[0]["draft"] = True
        with self.assertRaises(CheckError):
            self.announce()
        self.assertEqual(self.slack.calls, [])

    def test_corrupt_notes_and_state_fail_closed(self):
        self.announce()
        key = (1, "notification-state-000001.json")
        self.github.data.pop(key)
        with self.assertRaisesRegex(CheckError, "sequence"):
            self.announce()
        self.github.data[1, "notes.md"] = b"changed"
        with self.assertRaises(CheckError):
            self.announce()

    def test_historical_skipped(self):
        manifest = json.loads(self.github.data[1, "manifest.json"])
        manifest.pop("notification")
        self.github.data[1, "manifest.json"] = json.dumps(manifest).encode()
        self.announce()
        self.assertEqual(self.slack.calls, [])


class LongDelivery(LifecycleFixture, unittest.TestCase):
    def test_long_partial_thread_reconciles_in_order(self):
        notes = "## 1.0.1\n\n" + "- **Change** [PR](https://example.com/pr/1)\n" * 900
        (self.directory / "notes.md").write_text(notes)
        import library_release

        self.manifest["retained"]["notes.md"] = library_release.digest(
            self.directory / "notes.md"
        )
        self.manifest["notification"] = SCHEMA
        self.write_manifest()
        self.publish()
        slack = Messages()

        def send():
            announce(self.github, "v1.0.1", "C123", slack, lambda _: self.registries)

        original = slack.request

        def fail_second(method, **data):
            if method == "chat.postMessage" and len(slack.messages) == 1:
                slack.fail = "after"
            return original(method, **data)

        with (
            patch.object(slack, "request", side_effect=fail_second),
            self.assertRaises(CheckError),
        ):
            send()
        with (
            patch("release_notifications.LIMIT", 100),
            patch(
                "release_notifications.markdown",
                side_effect=AssertionError("rendered again"),
            ),
            patch(
                "release_notifications.chunks",
                side_effect=AssertionError("chunked again"),
            ),
        ):
            send()
            send()
        self.assertGreater(len(slack.messages), 2)
        root = slack.messages[0]["ts"]
        self.assertTrue(all(m["thread_ts"] == root for m in slack.messages[1:]))
        self.assertEqual(
            [m["metadata"]["event_payload"]["chunk"] for m in slack.messages],
            [str(i) for i in range(len(slack.messages))],
        )
        expected = (
            "*example 1.0.1*\n<https://github.com/fyndiq/example/releases/tag/v1.0.1|GitHub Release>\n\n"
            + markdown(notes)
        )
        self.assertEqual("".join(m["text"] for m in slack.messages), expected)


class Rendering(unittest.TestCase):
    def test_link_destinations_keep_markdown_characters(self):
        text = markdown(
            "**Docs** [**docs**](https://example.com/**path**?a=1&b=2) `**code**` [next](https://example.com/`literal`)"
        )
        self.assertEqual(
            text,
            "*Docs* <https://example.com/**path**?a=1&amp;b=2|*docs*> `**code**` <https://example.com/`literal`|next>",
        )

    def test_headings_and_emphasis_span_protected_links_and_code(self):
        self.assertEqual(
            markdown(
                "## [docs](https://example.com/**path**) and `code`\n**[next](https://example.com/a)**"
            ),
            "*<https://example.com/**path**|docs> and `code`*\n*<https://example.com/a|next>*",
        )

    def test_special_characters_links_code_and_mentions(self):
        text = markdown(
            "## Notes\n**bold** [PR](https://example.com/a?a=1&b=2) <@U123> <!channel> @here\n```\n## literal **bold**\n```\n`**literal**`"
        )
        self.assertIn("*Notes*", text)
        self.assertIn("<https://example.com/a?a=1&amp;b=2|PR>", text)
        self.assertIn("&lt;@U123&gt;", text)
        self.assertIn("&lt;!channel&gt;", text)
        self.assertIn("## literal **bold**", text)
        self.assertIn("`**literal**`", text)

    def test_long_content_and_links_complete(self):
        text = markdown(
            "[long](https://example.com/" + "x" * 5000 + ")\n" + "&<>" * 3000
        )
        parts = chunks(text)
        self.assertEqual("".join(parts), text)
        self.assertTrue(all(len(part) <= LIMIT for part in parts))
        self.assertTrue(
            all(
                not part.endswith(("&", "&a", "&am", "&amp", "&l", "&lt", "&g", "&gt"))
                for part in parts
            )
        )

    def test_code_fences_reopen(self):
        parts = chunks("```\n" + "literal **value**\n" * 500 + "```\n")
        self.assertTrue(all(len(p) <= LIMIT and p.count("```") % 2 == 0 for p in parts))
        self.assertEqual(sum(p.count("literal **value**") for p in parts), 500)


class Transport(unittest.TestCase):
    def test_missing_bot_token_fails_without_announcing(self):
        with (
            patch.dict(os.environ, {"SLACK_BOT_TOKEN": ""}),
            patch("release_notifications.announce") as send,
            self.assertRaisesRegex(
                CheckError, "Configure the existing Deploys bot token"
            ),
        ):
            from_environment(Mock(), "v1", "C123")
        send.assert_not_called()

    def test_announcement_cli_uses_dedicated_entry_point(self):
        import release_notifications

        with (
            patch.dict(os.environ, {"GH_TOKEN": "unused"}),
            patch(
                "sys.argv",
                [
                    "release_notifications.py",
                    "--repository",
                    "fyndiq/example",
                    "--tag",
                    "v1",
                ],
            ),
            patch("release_notifications.from_environment") as send,
        ):
            self.assertEqual(release_notifications.main(), 0)
        self.assertEqual(send.call_args.args[1:], ("v1", "C0CE6LY0T0C"))

    def test_announcement_cli_sanitizes_unexpected_errors(self):
        import release_notifications

        with (
            patch.dict(os.environ, {"GH_TOKEN": "unused"}),
            patch(
                "sys.argv",
                [
                    "release_notifications.py",
                    "--repository",
                    "fyndiq/example",
                    "--tag",
                    "v1",
                ],
            ),
            patch(
                "release_notifications.from_environment",
                side_effect=RuntimeError("secret"),
            ),
            patch("sys.stderr") as stderr,
        ):
            self.assertEqual(release_notifications.main(), 1)
        self.assertNotIn("secret", str(stderr.write.call_args_list))

    def test_rate_limits_and_failures(self):
        limited = Mock(status_code=429, headers={"Retry-After": "1"})
        success = Mock(status_code=200)
        success.json.return_value = {"ok": True, "ts": "1.1"}
        with (
            patch(
                "release_notifications.requests.post", side_effect=[limited, success]
            ),
            patch("release_notifications.time.sleep") as sleep,
        ):
            self.assertEqual(Slack("unused").request("chat.postMessage")["ts"], "1.1")
            sleep.assert_called_once_with(1)
        json_limited = Mock(status_code=200)
        json_limited.json.return_value = {"ok": False, "error": "ratelimited"}
        with (
            patch(
                "release_notifications.requests.post",
                side_effect=[json_limited, success],
            ),
            patch("release_notifications.time.sleep"),
        ):
            self.assertEqual(Slack("unused").request("chat.postMessage")["ts"], "1.1")
        invalid = Mock(status_code=200)
        invalid.json.side_effect = ValueError("secret")
        for response in (Mock(status_code=500), invalid):
            with patch("release_notifications.requests.post", return_value=response):
                with self.assertRaises(CheckError) as caught:
                    Slack("unused").request("chat.postMessage")
                self.assertNotIsInstance(caught.exception, Rejected)
        for error in ("internal_error", "fatal_error", "new_unknown_error"):
            response = Mock(status_code=200)
            response.json.return_value = {"ok": False, "error": error}
            with patch("release_notifications.requests.post", return_value=response):
                with self.assertRaises(CheckError) as caught:
                    Slack("unused").request("chat.postMessage")
                self.assertNotIsInstance(caught.exception, Rejected)
        with (
            patch(
                "release_notifications.requests.post",
                side_effect=requests.Timeout("secret"),
            ),
            self.assertRaisesRegex(CheckError, "response unavailable"),
        ):
            Slack("unused").request("chat.postMessage")

    def test_paginated_reconciliation_checks_author_and_thread(self):
        slack = Slack("unused")
        wanted = {
            "user": "UBOT",
            "text": "text",
            "ts": "1.2",
            "thread_ts": "1.1",
            "metadata": {
                "event_type": SCHEMA,
                "event_payload": {
                    "release": "id",
                    "chunk": "1",
                    "sha256": hashlib.sha256(b"text").hexdigest(),
                },
            },
        }
        for root in ("", "1.1"):
            with self.subTest(root=root):
                wanted["thread_ts"] = root or wanted["ts"]
                with patch.object(
                    slack,
                    "request",
                    side_effect=[
                        {
                            "messages": [
                                dict(wanted, user="OTHER"),
                                dict(wanted, thread_ts="OTHER"),
                            ],
                            "response_metadata": {"next_cursor": "next"},
                        },
                        {"messages": [wanted]},
                    ],
                ) as request:
                    self.assertEqual(
                        slack.find(
                            "C123", root, "id", 1, "UBOT", "text", oldest="1.000000"
                        ),
                        "1.2",
                    )
                    self.assertEqual(request.call_args.kwargs["cursor"], "next")
                    for call in request.call_args_list:
                        if root:
                            self.assertEqual(call.args, ("conversations.replies",))
                            self.assertEqual(call.kwargs["ts"], root)
                            self.assertNotIn("oldest", call.kwargs)
                            self.assertNotIn("inclusive", call.kwargs)
                        else:
                            self.assertEqual(call.args, ("conversations.history",))
                            self.assertEqual(call.kwargs["oldest"], "1.000000")
                            self.assertTrue(call.kwargs["inclusive"])


if __name__ == "__main__":
    unittest.main()
