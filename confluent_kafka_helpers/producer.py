import atexit
import socket

import structlog
from confluent_kafka import Producer as ConfluentProducer

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_on_delivery_cb, default_stats_cb, get_callback)
from confluent_kafka_helpers.serialization import Serializer

logger = structlog.get_logger(__name__)


class Producer:
    """
    Kafka producer with configurable key/value serializers.

    Does not subclass directly from Confluent's Producer,
    since it's a cimpl and therefore not mockable.
    """

    DEFAULT_CONFIG = {
        'acks': 'all',
        'api.version.request': True,
        'client.id': socket.gethostname(),
        'log.connection.close': False,
        'max.in.flight': 1,
        'queue.buffering.max.ms': 100,
        'statistics.interval.ms': 15000,
    }

    def __init__(self, config,
                 value_serializer=Serializer, key_serializer=Serializer,
                 get_callback=get_callback):  # yapf: disable
        config = {**self.DEFAULT_CONFIG, **config}
        config['on_delivery'] = get_callback(
            config.pop('on_delivery', None), default_on_delivery_cb
        )
        config['error_cb'] = get_callback(
            config.pop('error_cb', None), default_error_cb
        )
        config['stats_cb'] = get_callback(
            config.pop('stats_cb', None), default_stats_cb
        )

        self.value_serializer = config.pop('value.serializer', value_serializer)
        self.value_serializer = self.value_serializer(config)
        self.key_serializer = config.pop('key.serializer', key_serializer)
        self.key_serializer = self.key_serializer(config)

        for config_key in self.value_serializer.config_keys() +\
                self.key_serializer.config_keys():
            config.pop(config_key, None)

        topics = config.pop('topics')
        # use the first topic as default
        self.default_topic = next(iter(topics))

        logger.info("Initializing producer", config=config)
        atexit.register(self._close)

        self._producer_impl = self._init_producer_impl(config)

    @staticmethod
    def _init_producer_impl(config):
        return ConfluentProducer(config)

    def _close(self):
        logger.info("Flushing producer")
        self.flush()

    def flush(self, timeout=None):
        if timeout:
            self._producer_impl.flush(timeout)
        else:
            self._producer_impl.flush()

    def poll(self, timeout=None):
        if timeout:
            return self._producer_impl.poll(timeout)
        else:
            return self._producer_impl.poll()

    def produce(self, value, key=None, topic=None):
        topic = topic or self.default_topic
        value = self.value_serializer.serialize(value, topic)
        key = self.key_serializer.serialize(key, topic, is_key=True)

        logger.info("Producing message", topic=topic, key=key, value=value)
        self._produce(topic=topic, value=value, key=key)

    def _produce(self, topic, key, value, **kwargs):
        self._producer_impl.produce(topic=topic, value=value, key=key, **kwargs)
