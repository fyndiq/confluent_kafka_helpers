from unittest.mock import MagicMock as Mock

from confluent_kafka_helpers.metrics.callbacks import (
    StatsCallbackMetrics, error_cb_metrics, on_delivery_cb_metrics
)


class ErrorCallbackMetricsTests:
    def test_event_should_be_sent(self):
        statsd = Mock()
        logger = Mock()
        error_cb_metrics('foo', statsd, logger)

        assert statsd.event.called is True
        assert logger.critical.called is True


class OnDeliveryCallbackMetricsTests:
    def test_total_counter_increased(self):
        statsd = Mock()
        on_delivery_cb_metrics(None, 'foo', statsd, Mock())

        assert statsd.increment.called is True

    def test_error_counter_increased(self):
        statsd = Mock()
        logger = Mock()
        on_delivery_cb_metrics('error', Mock(), statsd, logger)

        assert statsd.increment.call_count == 2
        assert logger.error.called is True


class StatsCallbackMetricsTests:
    def test_metrics_should_be_sent(self):
        stats = Mock(
            brokers=[Mock()], topics=[Mock(partitions=[Mock()])]
        )
        statsd = Mock()
        StatsCallbackMetrics(stats, lambda s: s, statsd)

        assert statsd.gauge.call_count == 58
