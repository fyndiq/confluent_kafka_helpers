import atexit
import socket

import structlog
from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_on_delivery_cb, default_stats_cb, get_callback
)
from confluent_kafka_helpers.schema_registry.client import SchemaRegistryClient
from confluent_kafka_helpers.schema_registry.exceptions import SchemaNotFound
from confluent_kafka_helpers.schema_registry.subject import SubjectNameStrategies

logger = structlog.get_logger(__name__)


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """
    pass


class AvroProducer(ConfluentAvroProducer):
    DEFAULT_CONFIG = {
        'client.id': socket.gethostname(),
        'log.connection.close': False,
        'max.in.flight': 1,
        'queue.buffering.max.ms': 100,
        'statistics.interval.ms': 15000,
        'schemas.folder': 'schemas',
        'schemas.automatic.register': False,
        'schemas.key.subject.name.strategy': SubjectNameStrategies.TOPICRECORDNAME,
        'schemas.key.schema.default': {
            'type': 'string'
        },
        'schemas.value.subject.name.strategy': SubjectNameStrategies.TOPICRECORDNAME,
    }

    def __init__(self, config, schema_registry=SchemaRegistryClient, get_callback=get_callback):
        config = {**self.DEFAULT_CONFIG, **config}
        config['on_delivery'] = get_callback(
            config.pop('on_delivery', None), default_on_delivery_cb
        )
        config['error_cb'] = get_callback(config.pop('error_cb', None), default_error_cb)
        config['stats_cb'] = get_callback(config.pop('stats_cb', None), default_stats_cb)

        schemas_folder = config.pop('schemas.folder')
        automatic_register = config.pop('schemas.automatic.register')
        key_strategy = config.pop('schemas.key.subject.name.strategy')
        key_schema_default = config.pop('schemas.key.schema.default')
        value_strategy = config.pop('schemas.value.subject.name.strategy')

        self.schema_registry = schema_registry(
            url=config['schema.registry.url'], schemas_folder=schemas_folder,
            automatic_register=automatic_register, key_strategy=key_strategy,
            value_strategy=value_strategy
        )
        self.default_topic = config.pop('topics')[0]

        logger.info("Initializing producer", config=config)
        atexit.register(self._close)

        super().__init__(config, default_key_schema=key_schema_default)

    def _close(self):
        logger.info("Flushing producer")
        super().flush()

    def produce(self, value, key=None, topic=None, key_schema=None, value_schema=None, **kwargs):
        topic = topic or self.default_topic
        key_schema = self.schema_registry.get_latest_schema(
            topic=topic, schema=key_schema, is_key=True
        ) if key else None
        value_schema = self.schema_registry.get_latest_schema(topic=topic, schema=value_schema)
        logger.info("Producing message", topic=topic, key=key, value=value)
        breakpoint()
        super().produce(
            topic=topic, key=key, value=value, key_schema=key_schema, value_schema=value_schema,
            **kwargs
        )
