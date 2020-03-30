import atexit
import socket

import structlog
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from confluent_kafka_helpers import callbacks
from confluent_kafka_helpers.producer.serializer import encode_record_with_schema
from confluent_kafka_helpers.schema_registry.client import schema_registry
from confluent_kafka_helpers.schema_registry.subject import SubjectNameStrategies

logger = structlog.get_logger(__name__)


class AvroProducer(ConfluentAvroProducer):
    DEFAULT_CONFIG = {
        'client.id': socket.gethostname(),
        'log.connection.close': False,
        'max.in.flight': 1,
        'queue.buffering.max.ms': 100,
        'statistics.interval.ms': 15000,
        'schemas.folder': 'schemas',
        'schemas.automatic.register.enabled': False,
        'schemas.key.schema.default': avro.loads('{"type": "string"}'),
        'schemas.key.subject.name.strategy': SubjectNameStrategies.TOPICRECORDNAME,
        'schemas.value.subject.name.strategy': SubjectNameStrategies.TOPICRECORDNAME,
    }

    def __init__(
        self, config, schema_registry=schema_registry, get_callback=callbacks.get_callback
    ):
        config = {**self.DEFAULT_CONFIG, **config}
        config['on_delivery'] = get_callback(
            config.pop('on_delivery', None), callbacks.default_on_delivery_cb
        )
        config['error_cb'] = get_callback(config.pop('error_cb', None), callbacks.default_error_cb)
        config['stats_cb'] = get_callback(config.pop('stats_cb', None), callbacks.default_stats_cb)

        schemas_folder = config.pop('schemas.folder')
        automatic_register = config.pop('schemas.automatic.register.enabled')
        key_strategy = config.pop('schemas.key.subject.name.strategy')
        key_schema_default = config.pop('schemas.key.schema.default')
        value_strategy = config.pop('schemas.value.subject.name.strategy')

        schema_registry.init(
            url=config['schema.registry.url'], schemas_folder=schemas_folder,
            automatic_register=automatic_register, key_strategy=key_strategy,
            value_strategy=value_strategy
        )
        self.schema_registry = schema_registry
        self.default_topic = config.pop('topics')[0]
        logger.info("Initializing producer", config=config)
        atexit.register(self._close)

        MessageSerializer.encode_record_with_schema = encode_record_with_schema
        super().__init__(config, default_key_schema=key_schema_default)

    def _close(self):
        logger.info("Flushing producer")
        super().flush()

    def _get_schemas(self, key, topic, key_schema, value_schema):
        key_schema, *_ = self.schema_registry.get_latest_schema(
            topic=topic, schema=key_schema, is_key=True
        ) if key else None
        value_schema, *_ = self.schema_registry.get_latest_schema(topic=topic, schema=value_schema)
        schemas = {
            **({'key_schema': key_schema} if key_schema else {}),
            **({'value_schema': value_schema} if value_schema else {})
        }
        return schemas

    def produce(self, value, key=None, topic=None, key_schema=None, value_schema=None, **kwargs):
        topic = topic or self.default_topic
        schemas = self._get_schemas(
            key=key, topic=topic, key_schema=key_schema, value_schema=value_schema
        )
        logger.info("Producing message", topic=topic, key=key, value=value)
        super().produce(topic=topic, key=key, value=value, **schemas, **kwargs)
