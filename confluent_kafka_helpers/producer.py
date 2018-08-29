import atexit
from enum import Enum
import socket

import structlog
from confluent_kafka import Producer as ConfluentProducer

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_on_delivery_cb, default_stats_cb, get_callback)
from confluent_kafka_helpers.schema_registry import (
    AvroSchemaRegistry, SchemaNotFound)

logger = structlog.get_logger(__name__)


class SubjectNameStrategy(Enum):
    TopicNameStrategy = 0
    RecordNameStrategy = 1
    TopicRecordNameStrategy = 2


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """
    pass


class Serializer:
    def serialize(self, value, topic, **kwargs):
        return value


class AvroSerializer(Serializer):
    DEFAULT_CONFIG = {
        'auto.register.schemas': False,
        'key.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy,
        'value.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy
    }

    def __init__(self, config):
        config = {**self.DEFAULT_CONFIG, **config}

        schema_registry_url = config['schema.registry.url']
        self.schema_registry = schema_registry(schema_registry_url)
        self.auto_register_schemas = config.pop('auto.register.schemas')
        self.key_subject_name_strategy = config.pop(
            'key.subject.name.strategy'
        )
        self.value_subject_name_strategy = config.pop(
            'value.subject.name.strategy'
        )

    def _get_subject(self, topic, schema, strategy):
        if strategy == SubjectNameStrategy.TopicNameStrategy:
            return topic
        elif strategy == SubjectNameStrategy.RecordNameStrategy:
            return schema.fullname
        elif strategy == SubjectNameStrategy.TopicRecordNameStrategy:
            return '%{}-%{}'.format(topic, schema.fullname)
        else:
            raise ValueError('Unknown SubjectNameStrategy')

    def _ensure_schemas(self, topic, key_schema, value_schema):
        key_subject = self._get_subject(
            topic, key_schema, self.key_subject_name_strategy) + '-key'
        value_subject = self._get_subject(
            topic, value_schema, self.value_subject_name_strategy) + '-value'

        if self.auto_register_schemas:
            key_schema = self.schema_registry.register_schema(
                key_subject, key_schema)
            value_schema = self.schema_registry.register_schema(
                value_subject, value_schema)
        else:
            key_schema = self.schema_registry.get_latest_schema(key_subject)
            value_schema = self.schema_registry.get_latest_schema(value_subject)

        return key_schema, value_schema

    def serialize(self, value, **kwargs):
        key_schema, value_schema = self._ensure_schemas(
                topic, key_schema, value_schema)
        return


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
                 value_serializer=Serializer(), key_serializer=Serializer(),
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

        self.value_serializer = config.pop('value_serializer', value_serializer)
        self.key_serializer = config.pop('key_serializer', key_serializer)

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
        self._producer_impl.flush(timeout)

    def poll(self, timeout=None):
        return self._producer_impl.poll(timeout)

    def produce(self, value, key=None, topic=None):
        topic = topic or self.default_topic
        value = self.value_serializer.serialize(value, topic)
        key = self.value_serializer.serialize(key, topic, is_key=True)

        logger.info("Producing message", topic=topic, key=key, value=value)
        self._produce(topic=topic, value=value, key=key)

    def _produce(self, topic, key, value, **kwargs):
        self._producer_impl.produce(topic=topic, value=value, key=key, **kwargs)
