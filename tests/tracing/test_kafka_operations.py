from unittest.mock import Mock, call, patch

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
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


@pytest.mark.parametrize("has_parent", [False, True])
def test_poll_records_callback_failure_and_preserves_result(recording, avro_producer, has_parent):
    backend, exporter = recording
    with patch.object(producer.ConfluentAvroProducer, "poll", return_value=3) as poll:
        assert avro_producer.poll(0.1) is None
        poll.assert_called_once_with(0.1)
    assert exporter.get_finished_spans() == ()
    error = RuntimeError("delivery callback failed")

    def fail_poll():
        with patch.object(producer.ConfluentAvroProducer, "poll", side_effect=error):
            with patch.object(producer, "time_ns", return_value=100):
                with pytest.raises(RuntimeError) as raised:
                    avro_producer.poll(timeout=0)
        assert raised.value is error

    if has_parent:
        with backend.start_span("parent"):
            fail_poll()
    else:
        fail_poll()
    span = exporter.get_finished_spans()[0]
    assert span.name == "kafka.poll"
    assert span.start_time == 100
    assert bool(span.parent) is has_parent
    assert_error(span, error)


def test_commit_metrics_include_all_recovered_attempts(recording, avro_consumer, mocker):
    _, exporter = recording
    client = avro_consumer()
    statsd = mocker.patch.object(consumer, "statsd")
    error = KafkaException(KafkaError(KafkaError.REQUEST_TIMED_OUT))
    client.consumer.commit = Mock(side_effect=[error, error, None])
    mocker.patch.object(consumer, "monotonic", side_effect=[0, 0.1, 1, 1.2, 2, 2.3])
    assert client.commit(asynchronous=False) is None
    assert client.consumer.commit.call_count == 3
    spans = exporter.get_finished_spans()
    assert [span.name for span in spans] == ["kafka.commit", "kafka.commit"]
    for span in spans:
        assert_error(span, error)
    total = [args for args in statsd.increment.call_args_list if args.args[0].endswith("total")]
    failures = [args for args in statsd.increment.call_args_list if args.args[0].endswith("error")]
    assert len(total) == 3
    assert len(failures) == 2
    assert "mode:sync" in total[0].kwargs["tags"]
    assert "phase:attempt" in total[0].kwargs["tags"]
    assert "error_code:7" in failures[0].kwargs["tags"]
    durations = [args.args[1] for args in statsd.distribution.call_args_list]
    assert durations == pytest.approx([0.1, 0.2, 0.3])


def test_commit_partition_errors_do_not_change_retries_or_return(recording, avro_consumer, mocker):
    _, exporter = recording
    client = avro_consumer()
    statsd = mocker.patch.object(consumer, "statsd")
    error = KafkaError(KafkaError.REQUEST_TIMED_OUT)
    client.consumer.commit = Mock(return_value=[Mock(spec=TopicPartition, error=error)])
    assert client.commit(None, None, False) is None
    client.consumer.commit.assert_called_once_with(None, None, False)
    (span,) = exporter.get_finished_spans()
    assert_error(span, KafkaException(error))
    assert statsd.distribution.call_count == 1
    assert statsd.increment.call_count == 2


@pytest.mark.parametrize("partition_failure", [False, True])
def test_async_completion_errors_chain_user_callback_once(
    recording, avro_consumer, mocker, partition_failure
):
    _, exporter = recording
    callback = Mock()
    client = avro_consumer(config_override={"on_commit": callback})
    statsd = mocker.patch.object(consumer, "statsd")
    client.consumer.commit = Mock(return_value=None)
    client.commit(asynchronous=True)
    assert exporter.get_finished_spans() == ()
    error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
    overall = None if partition_failure else error
    partitions = [Mock(spec=TopicPartition, error=error if partition_failure else None)]
    client.config["on_commit"](overall, partitions)
    callback.assert_called_once_with(overall, partitions)
    (span,) = exporter.get_finished_spans()
    assert_error(span, KafkaException(error))
    assert span.attributes["commit.phase"] == "completion"
    assert statsd.distribution.call_count == 1  # Submission only.
    assert statsd.increment.call_args_list[-1] == call(
        "confluent_kafka_helpers.consumer.commit.count.error",
        tags=[
            "consumer_group:1",
            "mode:async_or_auto",
            "phase:completion",
            "outcome:error",
            f"error_code:{error.code()}",
        ],
    )


def test_successful_commit_completion_has_no_span(recording, avro_consumer, mocker):
    _, exporter = recording
    callback = Mock()
    client = avro_consumer(config_override={"on_commit": callback})
    statsd = mocker.patch.object(consumer, "statsd")
    client.config["on_commit"](None, [])
    callback.assert_called_once_with(None, [])
    assert exporter.get_finished_spans() == ()
    statsd.distribution.assert_not_called()
    statsd.increment.assert_called_once()


@pytest.mark.parametrize("has_parent", [False, True])
def test_commit_error_preserves_exception_parent_and_elapsed_time(
    recording, avro_consumer, mocker, has_parent
):
    backend, exporter = recording
    client = avro_consumer()
    statsd = mocker.patch.object(consumer, "statsd")
    error = KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN))
    client.consumer.commit = Mock(side_effect=error)
    mocker.patch.object(consumer, "time_ns", return_value=100)
    mocker.patch.object(consumer, "monotonic", side_effect=[0, 0.2])

    def fail_commit():
        with pytest.raises(KafkaException) as raised:
            client.commit(asynchronous=False)
        assert raised.value is error

    if has_parent:
        with backend.start_span("parent"):
            fail_commit()
    else:
        fail_commit()
    span = exporter.get_finished_spans()[0]
    assert_error(span, error)
    assert span.start_time == 100
    assert bool(span.parent) is has_parent
    assert statsd.distribution.call_args.args[1] == 0.2
    client.consumer.commit.assert_called_once_with(asynchronous=False)


def test_successful_sync_commit_counts_and_times_without_span(recording, avro_consumer, mocker):
    _, exporter = recording
    client = avro_consumer()
    statsd = mocker.patch.object(consumer, "statsd")
    client.consumer.commit = Mock(return_value=[TopicPartition("a", 0)])
    client.commit(asynchronous=False)
    assert exporter.get_finished_spans() == ()
    statsd.increment.assert_called_once()
    assert "outcome:success" in statsd.increment.call_args.kwargs["tags"]
    statsd.distribution.assert_called_once()


def test_on_commit_user_exception_is_not_swallowed(recording, avro_consumer):
    error = RuntimeError("user callback")
    callback = Mock(side_effect=error)
    client = avro_consumer(config_override={"on_commit": callback})
    with pytest.raises(RuntimeError) as raised:
        client.config["on_commit"](None, [])
    assert raised.value is error
    callback.assert_called_once_with(None, [])
