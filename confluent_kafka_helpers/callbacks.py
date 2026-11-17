from functools import partial

import structlog
from confluent_kafka import KafkaError as ConfluentKafkaError

from confluent_kafka_helpers.exceptions import KafkaDeliveryError, KafkaError
from confluent_kafka_helpers.metrics.callbacks import (
    error_cb_metrics,
    on_commit_cb_metrics,
    on_delivery_cb_metrics,
    stats_cb_metrics,
)
from confluent_kafka_helpers.utils import is_lost_assignment, is_retryable_commit_error

logger = structlog.get_logger(__name__)
INFORMATIONAL_ERRORS = {
    ConfluentKafkaError._TRANSPORT,
    ConfluentKafkaError._ALL_BROKERS_DOWN,
    ConfluentKafkaError._RESOLVE,
    ConfluentKafkaError._TIMED_OUT,
}


def get_callback(custom_callback, default_callback):
    if not custom_callback:
        return default_callback

    callback = partial(default_callback, custom_cb=custom_callback)
    return callback


def default_error_cb(error, custom_cb=None, send_metrics=error_cb_metrics):
    send_metrics(error)
    if custom_cb:
        custom_cb(error)
    if (
        isinstance(error, ConfluentKafkaError)
        and not error.fatal()
        and error.code() in INFORMATIONAL_ERRORS
    ):
        logger.warning("Kafka connection error; client will reconnect", error=str(error))
        return
    raise KafkaError(str(error))


def default_on_delivery_cb(error, message, custom_cb=None, send_metrics=on_delivery_cb_metrics):
    send_metrics(error, message)
    if custom_cb:
        custom_cb(error, message)
    if error:
        raise KafkaDeliveryError(str(error), message)


def default_on_commit_cb(error, partitions, custom_cb=None, send_metrics=on_commit_cb_metrics):
    send_metrics(error, partitions)
    if custom_cb:
        custom_cb(error, partitions)
    elif error is not None:
        if is_lost_assignment(error):
            logger.warning("Kafka assignment lost; offsets were not committed", error=str(error))
        elif is_retryable_commit_error(error):
            logger.warning("Kafka commit failed; offsets were not committed", error=str(error))
        else:
            logger.error("Kafka commit failed; offsets were not committed", error=str(error))


def default_stats_cb(stats, custom_cb=None, send_metrics=stats_cb_metrics):
    send_metrics(stats)
    if custom_cb:
        custom_cb(stats)
