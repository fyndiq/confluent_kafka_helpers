from functools import lru_cache

import structlog
from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from confluent_kafka_helpers.tracing import tracer

logger = structlog.get_logger(__name__)


class SchemaNotFound(Exception):
    pass


class AvroSchemaRegistry:
    def __init__(
        self, schema_registry_url, client=CachedSchemaRegistryClient, serializer=MessageSerializer
    ):
        self.client = client(url=schema_registry_url)
        self.serializer = serializer(self.client)

    def get_latest_schema(self, subject):
        with tracer.start_span(operation_name='kafka.producer.get_latest_schema') as span:
            span.set_tag('schema_registry.subject', subject)
            schema_id, schema, version = self.client.get_latest_schema(subject)
            if not schema:
                raise SchemaNotFound(f"Schema for subject {subject} not found")
        return schema

    @lru_cache(maxsize=None)
    def get_latest_cached_schema(self, subject):
        return self.get_latest_schema(subject)

    def key_serializer(self, subject, topic, key):
        schema = self.get_latest_cached_schema(subject)
        key = self.serializer.encode_record_with_schema(topic, schema, key, is_key=True)
        return key

    def register_schema(self, subject, avro_schema):
        logger.info("Registering schema", subject=subject, avro_schema=avro_schema)
        avro_schema = avro.load(avro_schema)
        schema_id = self.client.register(subject, avro_schema)
        logger.info("Registered schema with id", schema_id=schema_id)
        return schema_id
