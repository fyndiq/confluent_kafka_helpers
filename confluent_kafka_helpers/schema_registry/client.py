from functools import lru_cache

import structlog
from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from confluent_kafka_helpers.schema_registry.exceptions import SchemaNotFound
from confluent_kafka_helpers.schema_registry.subject import SubjectNameStrategies

logger = structlog.get_logger(__name__)


class SchemaRegistryClient:
    def __init__(
        self, url, topics=None, schemas_path=None, automatic_register=False,
        key_subject_name_strategy=SubjectNameStrategies.TOPICRECORDNAME,
        value_subject_name_strategy=SubjectNameStrategies.TOPICRECORDNAME,
        client=CachedSchemaRegistryClient, serializer=MessageSerializer
    ):
        self.client = client(url=url)
        self.serializer = serializer(self.client)
        self.topics = topics
        self.schemas_path = schemas_path
        self.key_subject_name_strategy = key_subject_name_strategy
        self.value_subject_name_strategy = value_subject_name_strategy

    def get_latest_schema(self, subject):
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

    def register_schema(self, subject, schema_file):
        logger.info("Registering schema", subject=subject, file=schema_file)
        schema = avro.load(schema_file)
        schema_id = self.client.register(subject, schema)
        logger.info("Registered schema with id", id=schema_id)
        return schema_id

    def test_compatibility(self, subject, schema_file):
        logger.info("Testing compatibility for schema", subject=subject, file=schema_file)
        schema = avro.load(schema_file)
        try:
            self.get_latest_schema(subject)
        except SchemaNotFound:
            logger.info("Schema not found, continuing")
            return True
        compatible = self.client.test_compatibility(subject=subject, avro_schema=schema)
        if not compatible:
            logger.error("Schema not compatible with latest version")
        else:
            logger.info("Schema is compatible with latest version")
        return compatible
