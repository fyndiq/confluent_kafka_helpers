from unittest.mock import Mock

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_on_delivery_cb, default_stats_cb, get_callback)


class GetCallBackTests:
    def test_should_return_partial_custom_callback(self):
        custom_cb = Mock()
        default_cb = Mock()
        callback = get_callback(custom_cb, default_cb)
        assert callback.func == default_cb
        assert callback.keywords == {'custom_cb': custom_cb}


class DefaultErrorCallbackTests:
    def test_should_send_metrics(self):
        send_metrics = Mock()
        default_error_cb(
            None, custom_cb=Mock(), send_metrics=send_metrics
        )
        send_metrics.assert_called_once_with(None)

    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_error_cb(None, custom_cb=custom_cb, send_metrics=Mock())
        custom_cb.assert_called_once_with(None)


class DefaultOnDeliveryCallbackTests:
    def test_should_send_metrics(self):
        send_metrics = Mock()
        default_on_delivery_cb(
            None, 2, custom_cb=Mock(), send_metrics=send_metrics
        )
        send_metrics.assert_called_once_with(None, 2)

    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_on_delivery_cb(
            None, 2, custom_cb=custom_cb, send_metrics=Mock()
        )
        custom_cb.assert_called_once_with(None, 2)


class DefaultStatsCallbackTests:
    def test_should_send_metrics(self):
        send_metrics = Mock()
        default_stats_cb(
            'foo', custom_cb=Mock(), send_metrics=send_metrics
        )
        send_metrics.assert_called_once_with('foo')

    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_stats_cb(
            None, custom_cb=custom_cb, send_metrics=Mock()
        )
        custom_cb.assert_called_once_with(None)
