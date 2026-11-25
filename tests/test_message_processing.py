import contextlib
import contextvars
from concurrent.futures import ThreadPoolExecutor

import pytest
from opentelemetry.trace import SpanKind

from confluent_kafka_helpers.context import get_propagated_headers, propagated_headers
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.tracing import attributes as attrs


class KafkaMessageStub:
    def __init__(
        self,
        *,
        headers,
        value=None,
        key="message-key",
        topic="articles",
        partition=2,
        offset=42,
    ):
        self._headers = headers
        self._value = value if value is not None else {"class": "ArticleProjected"}
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def timestamp(self):
        return (1, -1)

    def headers(self):
        return self._headers


class FakeSpan:
    def __init__(self, name, *, kind=None, resource_name=None, context=None):
        self.name = name
        self.kind = kind
        self.resource_name = resource_name
        self.context = context
        self.attributes = {}
        self.parent_span = None
        self.exit_args = None

    def set_attribute(self, key, value):
        self.attributes[key] = value


class FakeTracer:
    def __init__(self):
        self._active_span = contextvars.ContextVar("active_span", default=None)
        self.extracted_headers = []
        self.started_spans = []

    def active_span(self):
        return self._active_span.get()

    def extract_headers(self, headers):
        self.extracted_headers.append(dict(headers))
        return {"traceparent": headers["traceparent"]} if "traceparent" in headers else {}

    @contextlib.contextmanager
    def start_span(self, name, **kwargs):
        span = FakeSpan(name, **kwargs)
        span.parent_span = self._active_span.get()
        self.started_spans.append(span)
        token = self._active_span.set(span)
        try:
            yield span
        except Exception as exc:
            span.exit_args = (type(exc), exc)
            raise
        else:
            span.exit_args = (None, None)
        finally:
            self._active_span.reset(token)


@pytest.fixture
def fake_tracer(monkeypatch):
    tracer = FakeTracer()
    monkeypatch.setattr("confluent_kafka_helpers.message.tracer", tracer, raising=False)
    return tracer


def make_message(headers, *, value=None, key="message-key", offset=42):
    return Message(
        KafkaMessageStub(headers=headers, value=value, key=key, offset=offset),
        propagate_header_keys={"correlation_id"},
    )


def test_process_extracts_trace_from_all_headers_and_activates_downstream_spans(fake_tracer):
    message = make_message(
        [
            ("traceparent", b"00-11111111111111111111111111111111-1111111111111111-01"),
            ("correlation_id", b"corr-1"),
            ("not-propagated", b"kept-for-tracing"),
        ],
        key="article-1",
    )

    with message.process() as processing_span:
        assert get_propagated_headers() == {"correlation_id": "corr-1"}

        with fake_tracer.start_span("opensearch.request") as downstream_span:
            pass

    assert fake_tracer.extracted_headers == [
        {
            "traceparent": "00-11111111111111111111111111111111-1111111111111111-01",
            "correlation_id": "corr-1",
            "not-propagated": "kept-for-tracing",
        }
    ]
    assert processing_span.name == "kafka.process"
    assert processing_span.kind == SpanKind.CONSUMER
    assert processing_span.resource_name == "articles:ArticleProjected"
    assert processing_span.context == {
        "traceparent": "00-11111111111111111111111111111111-1111111111111111-01"
    }
    assert processing_span.attributes["message.class"] == "ArticleProjected"
    assert processing_span.attributes[attrs.MESSAGING_DESTINATION_NAME] == "articles"
    assert processing_span.attributes[attrs.MESSAGING_DESTINATION_PARTITION_ID] == 2
    assert processing_span.attributes[attrs.MESSAGING_KAFKA_MESSAGE_OFFSET] == 42
    assert processing_span.attributes[attrs.MESSAGING_KAFKA_MESSAGE_KEY] == "article-1"
    assert processing_span.attributes[attrs.MESSAGING_OPERATION_NAME] == "process"
    assert processing_span.attributes[attrs.MESSAGING_OPERATION_TYPE] == "process"
    assert downstream_span.parent_span is processing_span
    assert get_propagated_headers() == {}
    assert fake_tracer.active_span() is None


def test_process_isolates_concurrent_messages_with_different_trace_contexts(fake_tracer):
    messages = [
        make_message(
            [
                (
                    "traceparent",
                    b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-aaaaaaaaaaaaaaaa-01",
                ),
                ("correlation_id", b"corr-a"),
            ],
            offset=1,
        ),
        make_message(
            [
                (
                    "traceparent",
                    b"00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bbbbbbbbbbbbbbbb-01",
                ),
                ("correlation_id", b"corr-b"),
            ],
            offset=2,
        ),
    ]

    def handle(message):
        with message.process() as processing_span:
            with fake_tracer.start_span("handler.child") as child_span:
                return (
                    processing_span.context["traceparent"],
                    get_propagated_headers()["correlation_id"],
                    child_span.parent_span is processing_span,
                )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = set(executor.map(handle, messages))

    assert results == {
        ("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-aaaaaaaaaaaaaaaa-01", "corr-a", True),
        ("00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bbbbbbbbbbbbbbbb-01", "corr-b", True),
    }
    assert get_propagated_headers() == {}


def test_process_restores_context_when_worker_thread_is_reused(fake_tracer):
    messages = [
        make_message(
            [
                (
                    "traceparent",
                    b"00-11111111111111111111111111111111-1111111111111111-01",
                ),
                ("correlation_id", b"first"),
            ],
            offset=1,
        ),
        make_message([("correlation_id", b"second")], offset=2),
    ]

    def handle(message):
        before_headers = get_propagated_headers()
        before_span = fake_tracer.active_span()
        with message.process() as processing_span:
            inside_headers = get_propagated_headers()
            inside_span = fake_tracer.active_span()
        return before_headers, before_span, inside_headers, inside_span is processing_span

    with ThreadPoolExecutor(max_workers=1) as executor:
        first, second = list(executor.map(handle, messages))

    assert first == ({}, None, {"correlation_id": "first"}, True)
    assert second == ({}, None, {"correlation_id": "second"}, True)
    assert fake_tracer.started_spans[0].context == {
        "traceparent": "00-11111111111111111111111111111111-1111111111111111-01"
    }
    assert fake_tracer.started_spans[1].context == {}


def test_process_restores_nested_contexts(fake_tracer):
    first = make_message([("correlation_id", b"first")], offset=1)
    second = make_message([("correlation_id", b"second")], offset=2)

    with first.process() as first_span:
        assert get_propagated_headers() == {"correlation_id": "first"}
        assert fake_tracer.active_span() is first_span

        with second.process() as second_span:
            assert get_propagated_headers() == {"correlation_id": "second"}
            assert fake_tracer.active_span() is second_span

        assert get_propagated_headers() == {"correlation_id": "first"}
        assert fake_tracer.active_span() is first_span

    assert get_propagated_headers() == {}
    assert fake_tracer.active_span() is None


def test_process_restores_context_after_exceptions(fake_tracer):
    message = make_message([("correlation_id", b"inside")])
    error = RuntimeError("handler failed")

    with propagated_headers({"correlation_id": "outer"}):
        with pytest.raises(RuntimeError) as raised:
            with message.process() as processing_span:
                assert get_propagated_headers() == {"correlation_id": "inside"}
                assert fake_tracer.active_span() is processing_span
                raise error

        assert raised.value is error
        assert get_propagated_headers() == {"correlation_id": "outer"}

    assert fake_tracer.active_span() is None
    assert fake_tracer.started_spans[0].exit_args == (RuntimeError, error)


def test_process_handles_missing_trace_headers(fake_tracer):
    message = make_message([("correlation_id", b"corr-1"), ("foo", b"bar")])

    with message.process() as processing_span:
        with fake_tracer.start_span("handler.child") as child_span:
            pass

    assert fake_tracer.extracted_headers == [{"correlation_id": "corr-1", "foo": "bar"}]
    assert processing_span.context == {}
    assert child_span.parent_span is processing_span


def test_process_records_falsey_message_keys(fake_tracer):
    message = make_message([], key=0)

    with message.process() as processing_span:
        pass

    assert processing_span.attributes[attrs.MESSAGING_KAFKA_MESSAGE_KEY] == 0
