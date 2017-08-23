from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


def get_client(schema_registry_url):
    client = CachedSchemaRegistryClient(
        url=schema_registry_url
    )
    return client


def get_latest_schema(client, subject):
    schema_id, schema, version = client.get_latest_schema(subject)
    return schema


def avro_key_serializer(schema_registry_url, subject, topic, key):
    client = get_client(schema_registry_url)
    serializer = MessageSerializer(client)
    schema = get_latest_schema(client, subject)
    key = serializer.encode_record_with_schema(
        topic, schema, key, is_key=True
    )
    return key
