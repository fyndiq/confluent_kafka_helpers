import time
from unittest.mock import patch

import pytest

import confluent_kafka_helpers


class TestConsumerActionLatency:
    @patch("confluent_kafka_helpers.consumer.statsd")
    def test_emits_action_latency_after_message_is_handled(self, statsd, avro_consumer):
        started_at = int(time.time() * 1000) - 100
        headers = [
            ("action_started_at", str(started_at).encode()),
            ("primary_action_type", b"update_stock"),
        ]
        consumer = avro_consumer(
            config_override={"headers.propagate": ["action_started_at", "primary_action_type"]},
            headers=headers,
        )
        raw_message = consumer.consumer.poll()
        raw_message.value.return_value = {"class": "InventoryUpdated"}
        consumer.consumer.poll.side_effect = [raw_message, StopIteration]

        iterator = iter(consumer)
        next(iterator)
        assert statsd.distribution.call_count == 0

        confluent_kafka_helpers.set_shutdown_requested()
        with pytest.raises(StopIteration):
            next(iterator)

        metric_name, latency_ms = statsd.distribution.call_args.args
        assert metric_name == "confluent_kafka_helpers.consumer.action_latency"
        assert 100 <= latency_ms < 5_000
        assert statsd.distribution.call_args.kwargs == {
            "tags": [
                "consumer_group:1",
                "topic:test",
                "message_class:InventoryUpdated",
                "primary_action_type:update_stock",
            ]
        }

    @patch("confluent_kafka_helpers.consumer.statsd")
    def test_does_not_emit_action_latency_without_header(self, statsd, avro_consumer):
        consumer = avro_consumer()
        iterator = iter(consumer)

        next(iterator)
        confluent_kafka_helpers.set_shutdown_requested()
        with pytest.raises(StopIteration):
            next(iterator)

        statsd.distribution.assert_not_called()

    @patch("confluent_kafka_helpers.consumer.statsd")
    def test_does_not_emit_action_latency_when_handler_raises(self, statsd, avro_consumer):
        headers = [("action_started_at", str(int(time.time() * 1000)).encode())]
        consumer = avro_consumer(
            config_override={"headers.propagate": ["action_started_at"]}, headers=headers
        )

        with pytest.raises(RuntimeError):
            with consumer:
                for _ in consumer:
                    raise RuntimeError

        statsd.distribution.assert_not_called()

    @patch("confluent_kafka_helpers.consumer.statsd")
    def test_unparsable_action_started_at_does_not_raise(self, statsd, avro_consumer):
        consumer = avro_consumer(
            config_override={"headers.propagate": ["action_started_at"]},
            headers=[("action_started_at", b"not-an-epoch")],
        )
        iterator = iter(consumer)

        next(iterator)
        confluent_kafka_helpers.set_shutdown_requested()
        with pytest.raises(StopIteration):
            next(iterator)

        statsd.distribution.assert_not_called()
