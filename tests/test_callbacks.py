from unittest.mock import Mock

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_on_delivery_cb, get_callback)


class GetCallBackTests:
    def test_should_return_partial_custom_callback(self):
        custom_cb = Mock()
        default_cb = Mock()
        callback = get_callback(custom_cb, default_cb)
        assert callback.func == default_cb
        assert callback.keywords == {'custom_cb': custom_cb}


class DefaultErrorCallbackTests:
    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_error_cb(None, custom_cb=custom_cb)
        custom_cb.assert_called_once_with(None)


class DefaultOnDeliveryCallbackTests:
    def test_should_call_custom_callback(self):
        custom_cb = Mock()
        default_on_delivery_cb(None, 2, custom_cb=custom_cb)
        custom_cb.assert_called_once_with(None, 2)
