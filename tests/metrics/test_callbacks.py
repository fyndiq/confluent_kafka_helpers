import json
from unittest.mock import MagicMock, patch

from confluent_kafka_helpers.metrics.callbacks import (
    error_cb_metrics,
    on_delivery_cb_metrics,
    stats_cb_metrics,
)


class ErrorCallbackMetricsTests:
    def test_event_should_be_sent(self):
        statsd = MagicMock()
        error_cb_metrics("foo", statsd)

        assert statsd.event.called is True


class OnDeliveryCallbackMetricsTests:
    def test_total_counter_increased(self):
        statsd = MagicMock()
        on_delivery_cb_metrics(None, "foo", statsd)

        assert statsd.increment.called is True

    def test_error_counter_increased(self):
        statsd = MagicMock()
        on_delivery_cb_metrics("error", MagicMock(), statsd)

        assert statsd.increment.call_count == 2


class StatsCallbackMetricsTests:
    @patch("confluent_kafka_helpers.metrics.statistics.statsd")
    def test_metrics_should_be_sent(self, statsd):
        stats = {
            "cgrp": {"a": 1},
            "brokers": {"1": {"rtt": {"a": 1}, "int_latency": {"a": 1}, "throttle": {"a": 1}}},
        }
        stats_cb_metrics(json.dumps(stats))

        assert statsd.gauge.call_count == 53
