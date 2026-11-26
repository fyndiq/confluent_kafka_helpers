from contextlib import ExitStack
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from opentelemetry.trace import SpanKind, get_current_span

from confluent_kafka_helpers import producer
from confluent_kafka_helpers.context import propagated_headers_context
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


def test_many_preserves_each_output_source(
    avro_producer, confluent_avro_producer, confluent_message
):
    trace_ids = ["1" * 32, "2" * 32]
    first, second = [
        Message(
            confluent_message(
                headers=[("origin_action_type", action), ("traceparent", f"00-{tid}-{'a'*16}-01")]
            ),
            {"origin_action_type"},
        )
        for action, tid in zip([b"first", b"second"], trace_ids)
    ]
    with (
        propagated_headers_context({"origin_action_type": "ambient"}),
        patch.object(kafka_tracer, "start_span", wraps=kafka_tracer.start_span) as span,
    ):
        avro_producer.produce_many(
            [
                {"value": "output1", "source": first},
                {
                    "value": "output2",
                    "source": first,
                    "headers": {"origin_action_type": "override"},
                },
                {"value": "output3", "source": second},
            ]
        )
    assert [
        item.kwargs["headers"]["origin_action_type"]
        for item in confluent_avro_producer.call_args_list
    ] == ["first", "override", "second"]
    assert [
        get_current_span(item.kwargs["context"]).get_span_context().trace_id
        for item in span.call_args_list
    ] == [int(trace_ids[0], 16), int(trace_ids[0], 16), int(trace_ids[1], 16)]


def test_many_retries_only_rejected_record(avro_producer, confluent_avro_producer):
    confluent_avro_producer.side_effect = [None, BufferError(), None, None]
    with patch.object(producer.AvroProducer, "poll") as poll:
        avro_producer.produce_many([{"value": "first"}, {"value": "second"}, {"value": "third"}])
    assert [item.kwargs["value"] for item in confluent_avro_producer.call_args_list] == [
        "first",
        "second",
        "second",
        "third",
    ]
    poll.assert_called_once()


def test_many_fails_when_queue_stays_full(avro_producer, confluent_avro_producer):
    confluent_avro_producer.side_effect = BufferError()
    with pytest.raises(TimeoutError, match="queue remained full"):
        avro_producer.produce_many([{"value": "first"}], timeout=0)
    confluent_avro_producer.assert_called_once()


def test_delivery_scope_checks_callbacks_before_normal_exit(avro_producer, confluent_avro_producer):
    callback = MagicMock()
    with (
        patch.object(avro_producer, "_delivery_callback") as default_callback,
        patch.object(avro_producer, "flush") as flush,
    ):
        with avro_producer.delivery_scope() as delivery:
            delivery.produce_many([{"value": "output", "callback": callback}])
            delivered = confluent_avro_producer.call_args.kwargs["on_delivery"]
            flush.side_effect = lambda timeout: delivered(None, "sent") or 0
    flush.assert_called_once()
    default_callback.assert_called_once_with(None, "sent")
    callback.assert_called_once_with(None, "sent")


def test_delivery_scope_records_failure_reported_during_queue_poll(
    avro_producer, confluent_avro_producer
):
    confluent_avro_producer.side_effect = [None, BufferError(), None]
    with (
        patch.object(avro_producer, "_delivery_callback"),
        patch.object(avro_producer, "poll") as poll,
        pytest.raises(KafkaDeliveryError, match="delivery failed"),
    ):
        with avro_producer.delivery_scope() as delivery:
            delivery.produce_many([{"value": "first"}])
            delivered = confluent_avro_producer.call_args.kwargs["on_delivery"]
            poll.side_effect = lambda timeout: delivered("delivery failed", "first")
            delivery.produce_many([{"value": "second"}])
    assert confluent_avro_producer.call_count == 2
