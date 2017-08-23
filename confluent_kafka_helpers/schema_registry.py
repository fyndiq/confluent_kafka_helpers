from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


class SchemaRegistry:

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
