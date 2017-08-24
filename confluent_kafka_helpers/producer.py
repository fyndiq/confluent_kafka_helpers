from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers import logger
from confluent_kafka_helpers.schema_registry import SchemaRegistry


class AvroProducer:

    def __init__(self, producer_config):
        schema_registry_url = producer_config['schema.registry.url']
        key_subject_name = producer_config.pop('key_subject_name')
        value_subject_name = producer_config.pop('value_subject_name')

        # fetch latest schemas from schema registry
        schema_registry = SchemaRegistry(schema_registry_url)
        key_schema = schema_registry.get_latest_schema(key_subject_name)
        value_schema = schema_registry.get_latest_schema(value_subject_name)

        self.producer = ConfluentAvroProducer(
            producer_config,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def publish(self, topic, key, value, **kwargs):
        logger.info("Publishing message", topic=topic, key=key,
                    value=value)
        self.producer.produce(topic=topic, key=key, value=value)
        self.producer.flush()
