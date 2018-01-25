from functools import partial

import structlog

from confluent_kafka_helpers.metrics import base_metric, statsd

logger = structlog.get_logger(__name__)


def get_callback(custom_callback, default_callback):
    if not custom_callback:
        return default_callback

    callback = partial(
        default_callback,
        custom_cb=custom_callback
    )
    return callback


def default_error_cb(error, custom_cb=None):
    error_msg = "Kafka error"
    statsd.event(error_msg, str(error), alert_type='error')
    logger.critical(error_msg, error=str(error))
    if custom_cb:
        custom_cb(error)


def default_on_delivery_cb(error, message, custom_cb=None):
    statsd.increment(f'{base_metric}.producer.message.count.total')
    if error:
        statsd.increment(f'{base_metric}.producer.message.count.error')
        logger.error(
            "Produce failed", key=message.key(), value=message.value(),
            error=str(error)
        )
    if custom_cb:
        custom_cb(error, message)
