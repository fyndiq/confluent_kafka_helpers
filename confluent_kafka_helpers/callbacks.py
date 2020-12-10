from functools import partial

from confluent_kafka_helpers.exceptions import KafkaDeliveryError, KafkaError
from confluent_kafka_helpers.metrics.callbacks import (
    error_cb_metrics,
    on_delivery_cb_metrics,
    stats_cb_metrics,
)


def get_callback(custom_callback, default_callback):
    if not custom_callback:
        return default_callback

    callback = partial(default_callback, custom_cb=custom_callback)
    return callback


def default_error_cb(error, custom_cb=None, send_metrics=error_cb_metrics):
    send_metrics(error)
    if custom_cb:
        custom_cb(error)
    raise KafkaError(str(error))


def default_on_delivery_cb(error, message, custom_cb=None, send_metrics=on_delivery_cb_metrics):
    send_metrics(error, message)
    if custom_cb:
        custom_cb(error, message)
    if error:
        raise KafkaDeliveryError(str(error), message)


def default_stats_cb(stats, custom_cb=None, send_metrics=stats_cb_metrics):
    send_metrics(stats)
    if custom_cb:
        custom_cb(stats)
