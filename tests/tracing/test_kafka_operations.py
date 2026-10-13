from unittest.mock import Mock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from confluent_kafka_helpers import consumer, producer
from confluent_kafka_helpers.tracing import OpenTelemetryBackend

from tests import test_producer

avro_producer = test_producer.avro_producer
confluent_avro_producer = test_producer.confluent_avro_producer


@pytest.fixture
def recording(mocker):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    backend = OpenTelemetryBackend("test", tracer=provider.get_tracer)
    mocker.patch.object(consumer, "tracer", backend)
    mocker.patch.object(producer, "tracer", backend)
    yield backend, exporter
    provider.shutdown()


def assert_error(span, error):
    assert span.status.status_code == StatusCode.ERROR
    assert span.events[0].name == "exception"
    assert span.events[0].attributes["exception.message"] == str(error)


def test_consume_has_only_enclosing_span(recording, avro_consumer):
    _, exporter = recording
    client = avro_consumer()
    next(client)
    client._generator.close()
    spans = exporter.get_finished_spans()
    assert [span.name for span in spans] == ["kafka.consume"]


def test_produce_serialized_resource_and_propagation(
    recording, avro_producer, confluent_avro_producer
):
    _, exporter = recording
    avro_producer.value_serializer = lambda value: {"class": "Serialized", "value": value}
    avro_producer.produce("input", topic="a")
    spans = exporter.get_finished_spans()
    assert [span.name for span in spans] == ["kafka.produce"]
    span = spans[0]
    assert span.attributes["resource.name"] == "a:Serialized"
    assert span.attributes["message.class"] == "Serialized"
    headers = confluent_avro_producer.call_args.kwargs["headers"]
    assert headers["traceparent"].split("-")[1] == f"{span.context.trace_id:032x}"


@pytest.mark.parametrize("has_parent", [False, True])
def test_serialization_failure_marks_produce(recording, avro_producer, has_parent):
    backend, exporter = recording
    error = ValueError("serialization failed")
    avro_producer.value_serializer = Mock(side_effect=error)

    def produce_value():
        with pytest.raises(ValueError) as raised:
            avro_producer.produce("input")
        assert raised.value is error

    if has_parent:
        with backend.start_span("parent"):
            produce_value()
    else:
        produce_value()
    spans = exporter.get_finished_spans()
    assert spans[0].name == "kafka.produce"
    assert_error(spans[0], error)
    assert bool(spans[0].parent) is has_parent
    assert "kafka.serialize_message" not in [span.name for span in spans]


def test_poll_has_no_spans_and_preserves_exception(recording, avro_producer):
    _, exporter = recording
    with patch.object(producer.ConfluentAvroProducer, "poll", return_value=3) as poll:
        assert avro_producer.poll(0.1) is None
        poll.assert_called_once_with(0.1)

    error = RuntimeError("delivery callback failed")
    with patch.object(producer.ConfluentAvroProducer, "poll", side_effect=error) as poll:
        with pytest.raises(RuntimeError) as raised:
            avro_producer.poll(timeout=0)
        assert raised.value is error
        poll.assert_called_once_with(timeout=0)
    assert exporter.get_finished_spans() == ()


def test_commit_has_no_spans_and_preserves_exception(recording, avro_consumer):
    _, exporter = recording
    client = avro_consumer()
    client.consumer.commit = Mock(return_value=[Mock()])
    assert client.commit(None, None, False) is None
    client.consumer.commit.assert_called_once_with(None, None, False)

    error = KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN))
    client.consumer.commit = Mock(side_effect=error)
    with pytest.raises(KafkaException) as raised:
        client.commit(asynchronous=False)
    assert raised.value is error
    client.consumer.commit.assert_called_once_with(asynchronous=False)
    assert exporter.get_finished_spans() == ()


def test_on_commit_callback_is_registered_unchanged(recording, avro_consumer):
    _, exporter = recording
    error = RuntimeError("user callback")
    callback = Mock(side_effect=error)
    client = avro_consumer(config_override={"on_commit": callback})
    assert client.config["on_commit"] is callback
    assert client._mock_consumer.call_args.args[0]["on_commit"] is callback
    with pytest.raises(RuntimeError) as raised:
        client.config["on_commit"](None, [])
    assert raised.value is error
    callback.assert_called_once_with(None, [])
    assert exporter.get_finished_spans() == ()
