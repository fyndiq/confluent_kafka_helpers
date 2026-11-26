from unittest.mock import Mock, call, patch

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.avro.serializer import SerializerError
from opentelemetry.trace import get_current_span

from confluent_kafka_helpers import set_shutdown_requested
from confluent_kafka_helpers.consumer import AvroBatchConsumer
from confluent_kafka_helpers.context import (
    get_propagated_headers,
    message_context,
    propagated_headers_context,
)

from tests.config import Config


@pytest.fixture
def consumer_factory():
    native = Mock()
    native._serializer.decode_message.side_effect = lambda value, **kwargs: value.decode()
    native.commit.return_value = []
    with patch("confluent_kafka_helpers.consumer.ConfluentAvroConsumer", return_value=native):
        yield lambda **kwargs: AvroBatchConsumer(Config.KAFKA_CONSUMER_CONFIG, **kwargs)


def record(offset, partition=0, value=b"value", headers=None, error=None):
    raw = Mock()
    raw.topic.return_value = "a"
    raw.partition.return_value = partition
    raw.offset.return_value = offset
    raw.value.return_value = value
    raw.set_value.side_effect = lambda value: setattr(raw.value, "return_value", value)
    raw.key.return_value = b"key"
    raw.set_key.side_effect = lambda key: setattr(raw.key, "return_value", key)
    raw.headers.return_value = headers
    raw.timestamp.return_value = (0, -1)
    raw.error.return_value = error
    return raw


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.0, 1_000_001])
def test_invalid_batch_size(consumer_factory, batch_size):
    with pytest.raises(ValueError, match="batch_size"):
        consumer_factory(batch_size=batch_size)


@pytest.mark.parametrize("timeout", [True, -1, float("inf"), float("nan"), "1"])
def test_invalid_timeout(consumer_factory, timeout):
    with pytest.raises(ValueError, match="timeout"):
        consumer_factory(timeout=timeout)


@pytest.mark.parametrize("setting", ["enable.auto.commit", "enable.auto.offset.store"])
def test_rejects_auto_offsets(setting):
    with pytest.raises(ValueError, match=setting):
        AvroBatchConsumer({**Config.KAFKA_CONSUMER_CONFIG, setting: "true"})


def test_fetches_batches_in_order_without_losing_tombstones(consumer_factory):
    with consumer_factory(batch_size=2, timeout=0) as consumer:
        consumer.consumer.consume.side_effect = [
            [],
            [record(4, headers=[("origin_action_type", b"first")]), record(6, value=None)],
            [record(9, partition=2)],
        ]
        first, second = next(consumer)
        assert first.value == "value"
        assert first._meta.key == "key"
        assert first.propagated_headers == {"origin_action_type": "first"}
        assert second.value is None
        assert second.propagated_headers == {}
        (third,) = next(consumer)
        assert third._meta.partition == 2
        assert consumer.consumer.consume.call_args_list == [call(num_messages=2, timeout=0)] * 3
        assert consumer.config["enable.auto.commit"] is False
        assert consumer.config["enable.auto.offset.store"] is False

        consumer.commit_batch([second, first, third])
        consumer.consumer.commit.assert_called_once_with(
            offsets=[TopicPartition("a", 0, 7), TopicPartition("a", 2, 10)], asynchronous=False
        )
        assert [p.offset for p in consumer.consumer.commit.call_args.kwargs["offsets"]] == [7, 10]
        set_shutdown_requested()
        assert list(consumer) == []


def test_yields_remaining_data_before_stopping_at_eof():
    eof = KafkaError(KafkaError._PARTITION_EOF)
    native = Mock()
    native.assignment.return_value = [TopicPartition("a", 0), TopicPartition("a", 1)]
    native.consume.side_effect = [
        [record(1, error=eof)],
        [record(3, partition=1)],
        [record(4, partition=1, error=eof)],
    ]
    config = {**Config.KAFKA_CONSUMER_CONFIG, "stop_on_eof": True}
    with patch("confluent_kafka_helpers.consumer.ConfluentAvroConsumer", return_value=native):
        with AvroBatchConsumer(config, batch_size=1) as consumer:
            assert [[message._meta.offset for message in batch] for batch in consumer] == [[3]]
            assert consumer.config["enable.partition.eof"] is True


def test_eof_from_previous_assignment_does_not_stop_consumer():
    eof = KafkaError(KafkaError._PARTITION_EOF)
    native = Mock()
    native._serializer.decode_message.side_effect = lambda value, **kwargs: value.decode()
    moved = TopicPartition("a", 0)
    native.assignment.return_value = [moved, TopicPartition("a", 1)]

    def fetches():
        yield [record(1, error=eof)]
        callbacks = native.subscribe.call_args.kwargs
        callbacks["on_lost"](native, [moved])
        callbacks["on_assign"](native, [moved])
        yield [record(4, partition=1, error=eof)]
        yield [record(2), record(3, error=eof)]

    native.consume.side_effect = fetches()
    config = {**Config.KAFKA_CONSUMER_CONFIG, "stop_on_eof": True}
    with patch("confluent_kafka_helpers.consumer.ConfluentAvroConsumer", return_value=native):
        with AvroBatchConsumer(config) as consumer:
            assert [[message._meta.offset for message in batch] for batch in consumer] == [[2]]


@pytest.mark.parametrize("bad_record", ["broker", "avro"])
def test_failed_fetch_never_yields_or_fetches_again(consumer_factory, bad_record):
    with consumer_factory() as consumer:
        native = consumer.consumer
        if bad_record == "broker":
            native.consume.return_value = [
                record(1),
                record(2, error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
            ]
            expected = KafkaException
        else:
            native.consume.return_value = [record(1), record(2)]
            native._serializer.decode_message.side_effect = [
                "value",
                "key",
                SerializerError("invalid payload"),
            ]
            expected = SerializerError
        with pytest.raises(expected):
            next(consumer)
        assert list(consumer) == []
        native.consume.assert_called_once()


@pytest.mark.parametrize("partition_error", [True, False])
def test_batch_commit_propagates_lost_assignment(consumer_factory, partition_error):
    with consumer_factory() as consumer:
        consumer.consumer.consume.return_value = [record(1)]
        batch = next(consumer)
        error = KafkaError(KafkaError._ASSIGNMENT_LOST)
        if partition_error:
            consumer.consumer.commit.return_value = [Mock(error=error)]
        else:
            consumer.consumer.commit.side_effect = KafkaException(error)
        with pytest.raises(KafkaException) as exc:
            consumer.commit_batch(batch)
        assert exc.value.args[0] is error
        consumer.consumer.commit.assert_called_once()


def test_message_context_restores_headers_and_trace_on_exception(consumer_factory):
    trace_ids = ["1" * 32, "2" * 32]
    records = [
        record(
            i,
            headers=[
                ("origin_action_type", str(i).encode()),
                ("traceparent", f"00-{tid}-{'a'*16}-01"),
            ],
        )
        for i, tid in enumerate(trace_ids)
    ]
    with propagated_headers_context({"outside": "batch"}):
        with consumer_factory() as consumer:
            consumer.consumer.consume.return_value = records
            first, second = next(consumer)
            batch_span = get_current_span()
            assert get_propagated_headers() == {}
            with message_context(first):
                assert get_current_span().get_span_context().trace_id == int(trace_ids[0], 16)
                with pytest.raises(ValueError, match="handler"):
                    with message_context(second):
                        assert get_propagated_headers() == {"origin_action_type": "1"}
                        assert get_current_span().get_span_context().trace_id == int(
                            trace_ids[1], 16
                        )
                        raise ValueError("handler failed")
                assert get_propagated_headers() == {"origin_action_type": "0"}
                assert get_current_span().get_span_context().trace_id == int(trace_ids[0], 16)
            assert get_propagated_headers() == {}
            assert get_current_span() is batch_span
        assert get_propagated_headers() == {"outside": "batch"}
