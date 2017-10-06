from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers import logger
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry


class AvroProducer(ConfluentAvroProducer):

    DEFAULT_CONFIG = {
        'log.connection.close': False
    }

    def __init__(self, config, value_serializer=None):
        config.update(self.DEFAULT_CONFIG)
        schema_registry_url = config['schema.registry.url']

        self.default_topic = config.pop('default_topic')
        default_key_subject_name = f'{self.default_topic}-key'
        key_subject_name = config.pop(
            'key_subject_name', default_key_subject_name
        )
        default_value_subject_name = f'{self.default_topic}-value'
        value_subject_name = config.pop(
            'value_subject_name', default_value_subject_name
        )
        self.value_serializer = config.pop(
            'value_serializer', value_serializer
        )
        # fetch latest schemas from schema registry
        schema_registry = AvroSchemaRegistry(schema_registry_url)
        key_schema = schema_registry.get_latest_schema(key_subject_name)
        value_schema = schema_registry.get_latest_schema(value_subject_name)

        super().__init__(config, default_key_schema=key_schema,
                         default_value_schema=value_schema)

    def produce(self, key, value, topic=None, **kwargs):
        # TODO: fetch new schemas for topic (only once)
        topic = topic if topic else self.default_topic

        if self.value_serializer:
            value = self.value_serializer(value)

        logger.info("Producing message", topic=topic, key=key,
                    value=value)
        super().produce(topic=topic, key=key, value=value, **kwargs)
