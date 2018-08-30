import atexit
import socket
from enum import Enum

import structlog
from confluent_kafka import Producer as ConfluentProducer, avro
from confluent_kafka.avro import CachedSchemaRegistryClient, MessageSerializer

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_on_delivery_cb, default_stats_cb, get_callback)

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
    def __init__(self, config, **kwargs):
        pass

    def serialize(self, value, topic, **kwargs):
        return value


class AvroSerializer(Serializer):
    DEFAULT_CONFIG = {
        'auto.register.schemas': False,
        'key.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy,
        'value.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy
    }

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        config = {**self.DEFAULT_CONFIG, **config}
        schema_registry_url = config['schema.registry.url']
        self.schema_registry = CachedSchemaRegistryClient(schema_registry_url)
        self.auto_register_schemas = config['auto.register.schemas']
        self.key_subject_name_strategy = config['key.subject.name.strategy']
        self.value_subject_name_strategy = config['value.subject.name.strategy']
        self._serializer_impl = MessageSerializer(self.schema_registry)

    def _get_subject(self, topic, schema, strategy, is_key=False):
        if strategy == SubjectNameStrategy.TopicNameStrategy:
            subject = topic
        elif strategy == SubjectNameStrategy.RecordNameStrategy:
            subject = schema.fullname
        elif strategy == SubjectNameStrategy.TopicRecordNameStrategy:
            subject = '%{}-%{}'.format(topic, schema.fullname)
        else:
            raise ValueError('Unknown SubjectNameStrategy')

        subject += '-key' if is_key else '-value'
        return subject

    def _ensure_schema(self, topic, schema, is_key=False):
        subject = self._get_subject(
            topic, schema, self.key_subject_name_strategy, is_key)

        if self.auto_register_schemas:
            schema_id = self.schema_registry.register(subject, schema)
            schema = self.schema_registry.get_by_id(schema_id)
        else:
            schema_id, schema, _ = self.schema_registry.get_latest_schema(
                subject)

        return schema_id, schema

    def serialize(self, value, topic, is_key=False, **kwargs):
        schema_id, _ = self._ensure_schema(topic, value._schema, is_key)
        return self._serializer_impl.encode_record_with_schema_id(
            schema_id, value, is_key)


class AvroStringKeySerializer(AvroSerializer):
    """
    A specialized serializer for generic String keys,
    serialized with a simple value avro schema.
    """

    KEY_SCHEMA = avro.loads("""{"type": "string"}""")

    def serialize(self, value, topic, is_key=False, **kwargs):
        schema = self.KEY_SCHEMA if is_key else value._schema
        schema_id, _ = self._ensure_schema(topic, schema, is_key)
        return self._serializer_impl.encode_record_with_schema_id(
            schema_id, value, is_key)


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

        self.value_serializer = config.pop('value_serializer', value_serializer)
        self.value_serializer = self.value_serializer(config)
        self.key_serializer = config.pop('key_serializer', key_serializer)
        self.key_serializer = self.key_serializer(config)

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
        key = self.key_serializer.serialize(key, topic, is_key=True)

        logger.info("Producing message", topic=topic, key=key, value=value)
        self._produce(topic=topic, value=value, key=key)

    def _produce(self, topic, key, value, **kwargs):
        self._producer_impl.produce(topic=topic, value=value, key=key, **kwargs)
