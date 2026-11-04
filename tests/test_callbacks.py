from unittest.mock import Mock, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError

from confluent_kafka_helpers.callbacks import (
    default_error_cb,
    default_on_delivery_cb,
    default_stats_cb,
    get_callback,
)
from confluent_kafka_helpers.exceptions import KafkaDeliveryError, KafkaError


class GetCallBackTests:
    def test_should_return_partial_custom_callback(self):
        custom_cb = Mock()
        default_cb = Mock()
        callback = get_callback(custom_cb, default_cb)
        assert callback.func == default_cb
        assert callback.keywords == {"custom_cb": custom_cb}


class DefaultErrorCallbackTests:
    @pytest.mark.parametrize(
        "code",
        [
            ConfluentKafkaError._TRANSPORT,
            ConfluentKafkaError._ALL_BROKERS_DOWN,
            ConfluentKafkaError._RESOLVE,
            ConfluentKafkaError._TIMED_OUT,
        ],
    )
    def test_connection_errors_warn_and_keep_callbacks(self, code):
        error = ConfluentKafkaError(code)
        metrics, custom = Mock(), Mock()
        with patch("confluent_kafka_helpers.callbacks.logger") as logger:
            assert default_error_cb(error, custom, metrics) is None
        metrics.assert_called_once_with(error)
        custom.assert_called_once_with(error)
        logger.warning.assert_called_once()

    @pytest.mark.parametrize(
        "error",
        [
            ConfluentKafkaError(ConfluentKafkaError._TRANSPORT, fatal=True),
            ConfluentKafkaError(ConfluentKafkaError._AUTHENTICATION),
            ConfluentKafkaError(ConfluentKafkaError.TOPIC_AUTHORIZATION_FAILED),
            ConfluentKafkaError(ConfluentKafkaError.UNKNOWN_TOPIC_OR_PART),
            ConfluentKafkaError(ConfluentKafkaError._INVALID_ARG),
        ],
    )
    def test_other_errors_still_raise_after_callbacks(self, error):
        metrics, custom = Mock(), Mock()
        with pytest.raises(KafkaError):
            default_error_cb(error, custom, metrics)
        metrics.assert_called_once_with(error)
        custom.assert_called_once_with(error)

    def test_custom_callback_exception_is_not_suppressed(self):
        error = ConfluentKafkaError(ConfluentKafkaError._TRANSPORT)
        with pytest.raises(ValueError, match="custom"):
            default_error_cb(error, Mock(side_effect=ValueError("custom")), Mock())

    def test_should_send_metrics(self):
        send_metrics = Mock()
        with pytest.raises(KafkaError):
            default_error_cb(None, custom_cb=Mock(), send_metrics=send_metrics)
        send_metrics.assert_called_once_with(None)

    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        with pytest.raises(KafkaError):
            default_error_cb(None, custom_cb=custom_cb, send_metrics=Mock())
        custom_cb.assert_called_once_with(None)


class DefaultOnDeliveryCallbackTests:
    def test_should_send_metrics(self):
        send_metrics = Mock()
        default_on_delivery_cb(None, 2, custom_cb=Mock(), send_metrics=send_metrics)
        send_metrics.assert_called_once_with(None, 2)

    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_on_delivery_cb(None, 2, custom_cb=custom_cb, send_metrics=Mock())
        custom_cb.assert_called_once_with(None, 2)

    def test_should_raise_exception_on_error(self):
        error, message = Mock(), Mock()
        with pytest.raises(KafkaDeliveryError):
            default_on_delivery_cb(error, message, custom_cb=None, send_metrics=Mock())


class DefaultStatsCallbackTests:
    def test_should_send_metrics(self):
        send_metrics = Mock()
        default_stats_cb("foo", custom_cb=Mock(), send_metrics=send_metrics)
        send_metrics.assert_called_once_with("foo")

    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_stats_cb(None, custom_cb=custom_cb, send_metrics=Mock())
        custom_cb.assert_called_once_with(None)
