from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest
from opentelemetry.trace import SpanKind, get_current_span

from confluent_kafka_helpers import producer
from confluent_kafka_helpers.context import propagated_headers
from confluent_kafka_helpers.exceptions import KafkaDeliveryError
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.tracing import tracer as kafka_tracer

from tests import config


@pytest.fixture
def confluent_avro_producer():
    producer = MagicMock()
    with ExitStack() as stack:
        stack.enter_context(
            patch("confluent_kafka_helpers.producer.ConfluentAvroProducer.__init__", MagicMock)
        )
        stack.enter_context(
            patch("confluent_kafka_helpers.producer.ConfluentAvroProducer.produce", producer)
        )
        yield producer


@pytest.fixture
@patch("confluent_kafka_helpers.producer.AvroProducer._close", MagicMock())
def avro_producer(confluent_avro_producer, avro_schema_registry):
    producer_config = config.Config.KAFKA_PRODUCER_CONFIG
    producer_config["client.id"] = "<client-id>"
    return producer.AvroProducer(producer_config, schema_registry=avro_schema_registry)


def test_avro_producer_init(avro_producer, avro_schema_registry):
    producer_config = config.Config.KAFKA_PRODUCER_CONFIG
    assert avro_producer.default_topic == "c"
    assert avro_producer.value_serializer == config.to_message_from_dto
    avro_schema_registry.assert_called_once_with(producer_config["schema.registry.url"])


def test_avro_producer_produce_default_topic(confluent_avro_producer, avro_producer):
    key = "a"
    value = "1"
    topic = "c"
    avro_producer.produce(key=key, value=value)

    _, key_schema, value_schema = avro_producer.topic_schemas[topic]
    default_topic = avro_producer.default_topic
    confluent_avro_producer.assert_called_once_with(
        topic=default_topic,
        key=key,
        value=avro_producer.value_serializer(value),
        key_schema=key_schema,
        value_schema=value_schema,
        headers={},
    )


def test_avro_producer_produce_specific_topic(confluent_avro_producer, avro_producer):
    key = "a"
    value = "1"
    topic = "a"
    avro_producer.produce(key=key, value=value, topic=topic)

    topic, key_schema, value_schema = avro_producer.topic_schemas[topic]
    confluent_avro_producer.assert_called_once_with(
        topic=topic,
        key=key,
        value=avro_producer.value_serializer(value),
        key_schema=key_schema,
        value_schema=value_schema,
        headers={},
    )


@patch("confluent_kafka_helpers.producer.tracer")
def test_avro_producer_adds_tracing(tracer, avro_producer):
    avro_producer.produce(key="a", value="1", topic="a")
    expected_calls = [
        call.start_span(name="kafka.produce", kind=SpanKind.PRODUCER, resource_name="a"),
        call.start_span().__enter__(),
        call.start_span().__enter__().set_attribute("resource.name", "a"),
        call.inject_headers(headers={}),
        call.start_span().__enter__().set_attribute("messaging.operation.name", "produce"),
        call.start_span().__enter__().set_attribute("messaging.operation.type", "publish"),
        call.start_span().__enter__().set_attribute("messaging.destination.name", "a"),
        call.start_span().__enter__().set_attribute("messaging.client.id", "<client-id>"),
        call.start_span().__enter__().set_attribute("messaging.kafka.message.tombstone", False),
        call.start_span().__enter__().set_attribute("messaging.kafka.message.key", "a"),
        call.start_span().__enter__().set_attribute("server.address", "localhost"),
        call.start_span().__enter__().set_attribute("server.port", "9092"),
        call.start_span()
        .__enter__()
        .set_attribute("confluent_kafka_helpers.producer.service.name", "unknown_service"),
        call.start_span().__exit__(None, None, None),
    ]
    tracer.assert_has_calls(expected_calls)


@patch("confluent_kafka_helpers.producer.tracer")
def test_serializer_failure_exits_produce_span(tracer, avro_producer, confluent_avro_producer):
    error = ValueError("serialization failed")
    avro_producer.value_serializer = MagicMock(side_effect=error)

    with pytest.raises(ValueError) as raised:
        avro_producer.produce("input", topic="a")

    assert raised.value is error
    tracer.start_span.assert_called_once_with(
        name="kafka.produce", kind=SpanKind.PRODUCER, resource_name="a"
    )
    tracer.start_span.return_value.__exit__.assert_called_once_with(ValueError, error, ANY)
    confluent_avro_producer.assert_not_called()


def test_get_subject_names(avro_producer):
    topic_name = "test_topic"
    key_subject_name, value_subject_name = avro_producer._get_subject_names(topic_name)
    assert key_subject_name == (topic_name + "-key")
    assert value_subject_name == (topic_name + "-value")


def test_get_topic_schemas(avro_producer, avro_schema_registry):
    avro_schema_registry.return_value.get_latest_schema.side_effect = ["1", "2"]
    topic_list = ["a"]
    topic_schemas = avro_producer._get_topic_schemas(topic_list)
    topic, key_schema, value_schema = topic_schemas["a"]
    assert topic == "a"
    assert key_schema == "1"
    assert value_schema == "2"


@patch("confluent_kafka_helpers.producer.get_propagated_headers")
def test_producer_merges_context_and_explicit_headers(
    get_propagated_headers, confluent_avro_producer, avro_producer
):
    get_propagated_headers.return_value = {
        "x-request-id": "abc-123",
        "trace-id": "trace-xyz",
    }

    avro_producer.produce(
        key="k",
        value="v",
        topic="c",
        headers={"trace-id": "user-overridden", "user-id": "user-456"},
    )

    expected_headers = {
        "x-request-id": "abc-123",  # from context
        "trace-id": "user-overridden",  # overridden by user
        "user-id": "user-456",  # only in user
    }

    _, key_schema, value_schema = avro_producer.topic_schemas["c"]
    confluent_avro_producer.assert_called_once_with(
        topic="c",
        key="k",
        value=avro_producer.value_serializer("v"),
        key_schema=key_schema,
        value_schema=value_schema,
        headers=expected_headers,
    )


@patch("confluent_kafka_helpers.producer.tracer")
@patch("confluent_kafka_helpers.producer.get_propagated_headers")
def test_producer_adds_propagated_headers_to_span(get_propagated_headers, tracer, avro_producer):
    get_propagated_headers.return_value = {
        "x-request-id": "abc-123",
        "correlation_id": "xyz-789",
    }

    avro_producer.produce(key="a", value="1", topic="a")

    tracer.start_span().__enter__().set_attribute.assert_any_call(
        "confluent_kafka_helpers.header.x-request-id", "abc-123"
    )
    tracer.start_span().__enter__().set_attribute.assert_any_call(
        "messaging.message.conversation_id", "xyz-789"
    )


def test_many_preserves_sources(avro_producer, confluent_avro_producer, confluent_message):
    sources = [
        Message(
            confluent_message(
                headers=[
                    ("origin_action_type", action),
                    ("traceparent", f"00-{tid * 32}-{'a' * 16}-01"),
                ]
            ),
            {"origin_action_type"},
        )
        for action, tid in [(b"first", "1"), (b"second", "2")]
    ]
    with (
        propagated_headers({"origin_action_type": "ambient"}),
        patch.object(kafka_tracer, "start_span", wraps=kafka_tracer.start_span) as span,
    ):
        avro_producer.produce_many(
            [
                {"value": "one", "source": sources[0]},
                {
                    "value": "two",
                    "source": sources[0],
                    "headers": {"origin_action_type": "override"},
                },
                {"value": "three", "source": sources[1]},
            ]
        )
    assert [
        c.kwargs["headers"]["origin_action_type"] for c in confluent_avro_producer.call_args_list
    ] == ["first", "override", "second"]
    assert [
        get_current_span(c.kwargs["context"]).get_span_context().trace_id
        for c in span.call_args_list
    ] == [int("1" * 32, 16)] * 2 + [int("2" * 32, 16)]


def test_many_retries_only_rejected_record(avro_producer, confluent_avro_producer):
    confluent_avro_producer.side_effect = [None, BufferError(), None, None]
    with patch.object(avro_producer, "poll") as poll:
        avro_producer.produce_many([{"value": v} for v in ["first", "second", "third"]])
    assert [c.kwargs["value"] for c in confluent_avro_producer.call_args_list] == [
        "first",
        "second",
        "second",
        "third",
    ]
    poll.assert_called_once()


def test_many_bounds_queue_wait(avro_producer, confluent_avro_producer):
    confluent_avro_producer.side_effect = BufferError()
    with pytest.raises(TimeoutError, match="queue remained full"):
        avro_producer.produce_many([{"value": "output"}], timeout=0)
    confluent_avro_producer.assert_called_once()


def test_scope_confirms_ordinary_and_collection_output(avro_producer, confluent_avro_producer):
    callback = Mock()
    with (
        patch.object(avro_producer, "_delivery_callback") as default_callback,
        patch.object(avro_producer, "flush") as flush,
    ):

        def delivered(timeout):
            for c in confluent_avro_producer.call_args_list:
                c.kwargs["on_delivery"](None, "sent")
            return 0

        flush.side_effect = delivered
        with avro_producer.delivery_scope() as delivery:
            assert avro_producer.in_delivery_scope
            avro_producer.produce("ordinary", callback=callback)
            delivery.produce_many([{"value": "collection"}])
    flush.assert_called_once()
    assert default_callback.call_count == 2
    callback.assert_called_once_with(None, "sent")
    assert not avro_producer.in_delivery_scope


@pytest.mark.parametrize("failure", ["delivery", "callback", "pending", "outstanding"])
def test_scope_rejects_unconfirmed_output(avro_producer, confluent_avro_producer, failure):
    callback = Mock(side_effect=ValueError("callback failed") if failure == "callback" else None)

    def flush(timeout):
        if failure != "pending":
            confluent_avro_producer.call_args.kwargs["on_delivery"](
                "delivery failed" if failure == "delivery" else None, "sent"
            )
        return int(failure == "outstanding")

    with (
        patch.object(avro_producer, "_delivery_callback"),
        patch.object(avro_producer, "flush", side_effect=flush),
        pytest.raises((KafkaDeliveryError, ValueError, TimeoutError)),
    ):
        with avro_producer.delivery_scope():
            avro_producer.produce("output", on_delivery=callback)
    assert not avro_producer.in_delivery_scope


def test_scope_catches_early_callback_failure(avro_producer, confluent_avro_producer):
    confluent_avro_producer.side_effect = [None, BufferError(), None]
    with (
        patch.object(avro_producer, "_delivery_callback"),
        patch.object(avro_producer, "poll") as poll,
        pytest.raises(KafkaDeliveryError, match="delivery failed"),
    ):
        with avro_producer.delivery_scope():
            avro_producer.produce("first")
            delivered = confluent_avro_producer.call_args.kwargs["on_delivery"]
            poll.side_effect = lambda timeout: delivered("delivery failed", "first")
            avro_producer.produce("second")
    assert confluent_avro_producer.call_count == 2


def test_scope_bounds_ordinary_queue_wait(avro_producer, confluent_avro_producer):
    confluent_avro_producer.side_effect = BufferError()
    with (
        patch.object(avro_producer, "poll"),
        patch(
            "confluent_kafka_helpers.producer.time.monotonic",
            side_effect=[0, 0, 0, 0.05, 1],
        ),
        pytest.raises(TimeoutError),
    ):
        with avro_producer.delivery_scope(timeout=0.1):
            avro_producer.produce("output")


def test_scope_restores_state_on_handler_failure(avro_producer):
    with patch.object(avro_producer, "flush") as flush, pytest.raises(ValueError):
        with avro_producer.delivery_scope():
            raise ValueError("handler failed")
    flush.assert_not_called()
    assert not avro_producer.in_delivery_scope


def test_scope_rejects_nesting(avro_producer):
    with avro_producer.delivery_scope():
        with pytest.raises(RuntimeError, match="Nested"):
            with avro_producer.delivery_scope():
                pass


def test_scope_tracks_joined_worker_output(avro_producer, confluent_avro_producer):
    with (
        patch.object(avro_producer, "_delivery_callback"),
        patch.object(avro_producer, "flush") as flush,
    ):

        def delivered(timeout):
            for c in confluent_avro_producer.call_args_list:
                c.kwargs["on_delivery"](None, "sent")
            return 0

        flush.side_effect = delivered
        with avro_producer.delivery_scope():
            with ThreadPoolExecutor(max_workers=2) as workers:
                list(workers.map(avro_producer.produce, ["one", "two"]))
    assert confluent_avro_producer.call_count == 2
    flush.assert_called_once()


def test_scope_retains_caught_enqueue_failure(avro_producer):
    with patch.object(avro_producer, "_produce_once", side_effect=ValueError("bad output")):
        with pytest.raises(ValueError, match="bad output"):
            with avro_producer.delivery_scope():
                with pytest.raises(ValueError):
                    avro_producer.produce("output")
