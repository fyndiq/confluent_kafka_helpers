from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers import logger
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry


class AvroProducer(ConfluentAvroProducer):

    def __init__(self, producer_config, value_serializer=None):
        schema_registry_url = producer_config['schema.registry.url']

        self.default_topic = producer_config.pop('default_topic')
        default_key_subject_name = f'{self.default_topic}-key'
        key_subject_name = producer_config.pop(
            'key_subject_name', default_key_subject_name
        )
        default_value_subject_name = f'{self.default_topic}-value'
        value_subject_name = producer_config.pop(
            'value_subject_name', default_value_subject_name
        )
        self.value_serializer = producer_config.pop(
            'value_serializer', value_serializer
        )
        # fetch latest schemas from schema registry
        schema_registry = AvroSchemaRegistry(schema_registry_url)
        key_schema = schema_registry.get_latest_schema(key_subject_name)
        value_schema = schema_registry.get_latest_schema(value_subject_name)

        super().__init__(producer_config, default_key_schema=key_schema,
                         default_value_schema=value_schema)

    def produce(self, key, value, topic=None):
        # TODO: fetch new schemas for topic (only once)
        topic = topic if topic else self.default_topic

        if self.value_serializer:
            value = self.value_serializer(value)

        logger.info("Producing message", topic=topic, key=key,
                    value=value)
        super().produce(topic=topic, key=key, value=value)
        super().flush()
