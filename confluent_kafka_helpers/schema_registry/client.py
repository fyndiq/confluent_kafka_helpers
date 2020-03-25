from functools import lru_cache

import structlog
from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from confluent_kafka_helpers.schema_registry import subject, utils
from confluent_kafka_helpers.schema_registry.exceptions import SchemaNotFound

logger = structlog.get_logger(__name__)


class SchemaRegistryClient:
    def __init__(
        self, url, schemas_folder=None, automatic_register=False,
        key_strategy=subject.SubjectNameStrategies.TOPICRECORDNAME,
        value_strategy=subject.SubjectNameStrategies.TOPICRECORDNAME,
        client=CachedSchemaRegistryClient, serializer=MessageSerializer,
        resolver=subject.SubjectNameResolver
    ):
        self._client = client(url=url)
        self._serializer = serializer(self._client)
        self._schemas_folder = schemas_folder
        self._automatic_register = automatic_register
        self._key_strategy = key_strategy
        self._value_strategy = value_strategy
        self._resolver = resolver

    def get_latest_schema(self, schema=None, subject=None, topic=None, is_key=False):
        if not subject:
            resolver = self._resolver(
                strategy=self._key_strategy if is_key else self._value_strategy
            )
            subject = resolver.get_subject(schema=schema, is_key=is_key, topic=topic)
        if not subject:
            return
        schema_id, schema, version = self._client.get_latest_schema(subject)
        if not schema:
            raise SchemaNotFound(f"Schema for subject {subject} not found")
        return schema

    @lru_cache(maxsize=None)
    def get_latest_cached_schema(self, subject):
        return self.get_latest_schema(subject)

    def key_serializer(self, subject, topic, key):
        schema = self.get_latest_cached_schema(subject)
        key = self._serializer.encode_record_with_schema(topic, schema, key, is_key=True)
        return key

    def register_schema(self, schema_file, subject=None, is_key=False):
        schema = avro.load(schema_file)
        if not subject:
            resolver = self._resolver(
                strategy=self._key_strategy if is_key else self._value_strategy
            )
            subject = resolver.get_subject(
                schema=schema, is_key=is_key,
                topic=utils.get_topic_from_schema_file(schema_file=schema_file)
            )
        logger.info("Registering schema", subject=subject, schema=schema)
        schema_id = self._client.register(subject, schema)
        logger.info("Registered schema with id", id=schema_id)
        return schema_id

    def test_compatibility(self, schema_file, subject=None, is_key=False):
        schema = avro.load(schema_file)
        topic = utils.get_topic_from_schema_file(schema_file=schema_file)
        if not subject:
            resolver = self._resolver(
                strategy=self._key_strategy if is_key else self._value_strategy
            )
            subject = resolver.get_subject(schema=schema, is_key=is_key, topic=topic)
        logger.info("Testing compatibility for schema", subject=subject, fullname=schema.fullname)
        try:
            self.get_latest_schema(schema=schema, subject=subject, topic=topic, is_key=is_key)
        except SchemaNotFound:
            logger.info("Schema not found, continuing")
            return True
        compatible = self._client.test_compatibility(subject=subject, avro_schema=schema)
        if not compatible:
            logger.error("Schema not compatible with latest version")
        else:
            logger.info("Schema is compatible with latest version")
        return compatible
