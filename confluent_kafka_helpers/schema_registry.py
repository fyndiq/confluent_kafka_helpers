from confluent_kafka import avro

from confluent_kafka_helpers import logger
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


class AvroSchemaRegistry:

    def __init__(self, schema_registry_url):
        self.client = CachedSchemaRegistryClient(
            url=schema_registry_url
        )

    def get_latest_schema(self, subject):
        schema_id, schema, version = self.client.get_latest_schema(subject)
        return schema

    def key_serializer(self, subject, topic, key):
        serializer = MessageSerializer(self.client)
        schema = self.get_latest_schema(subject)
        key = serializer.encode_record_with_schema(
            topic, schema, key, is_key=True
        )
        return key

    def register_schema(self, subject, avro_schema):
        logger.info("Registering schema", subject=subject,
                    avro_schema=avro_schema)
        avro_schema = avro.load(avro_schema)
        schema_id = self.client.register(subject, avro_schema)
        logger.info("Registered schema with id", schema_id=schema_id)

        return schema_id
