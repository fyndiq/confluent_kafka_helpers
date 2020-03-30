from functools import lru_cache

import structlog
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from confluent_kafka_helpers.schema_registry import exceptions, subject
from confluent_kafka_helpers.schema_registry.cache import schema_cache

logger = structlog.get_logger(__name__)


class SchemaRegistryClient:
    def init(
        self, url, schemas_folder=None, automatic_register=False,
        key_strategy=subject.SubjectNameStrategies.TOPICRECORDNAME,
        value_strategy=subject.SubjectNameStrategies.TOPICRECORDNAME,
        client=CachedSchemaRegistryClient, serializer=MessageSerializer,
        resolver=subject.SubjectNameResolver, cache=schema_cache
    ):
        self._client = client(url=url)
        self._serializer = serializer(self._client)
        self._schemas_folder = schemas_folder
        self._automatic_register = automatic_register
        self._key_strategy = key_strategy
        self._value_strategy = value_strategy
        self._resolver = resolver
        self._cache = cache

    @property
    def cache(self):
        return self._cache

    def _resolve_subject(self, subject, schema, topic, is_key):
        if subject:
            return subject
        resolver = self._resolver(strategy=self._key_strategy if is_key else self._value_strategy)
        subject = resolver.get_subject(schema=schema, is_key=is_key, topic=topic)
        return subject

    @lru_cache(maxsize=None)
    def get_latest_cached_schema(self, subject):
        return self.get_latest_schema(subject)

    def key_serializer(self, subject, topic, key):
        schema = self.get_latest_cached_schema(subject)
        key = self._serializer.encode_record_with_schema(topic, schema, key, is_key=True)
        return key

    def get_latest_schema(self, schema, subject=None, topic=None, is_key=False):
        if cached_schema := self._cache.get(schema=schema):
            return cached_schema

        subject = self._resolve_subject(subject=subject, schema=schema, topic=topic, is_key=is_key)
        if not subject:
            return None, None, None

        schema_id, schema, version = self._client.get_latest_schema(subject)
        if not schema:
            raise exceptions.SchemaNotFound(f"Schema for subject {subject} not found")
        self._cache.update(schema=schema, schema_id=schema_id, subject=subject)

        return schema, schema_id, subject

    def register_schema(self, schema, subject=None, topic=None, is_key=False):
        subject = self._resolve_subject(subject=subject, schema=schema, topic=topic, is_key=is_key)
        logger.info("Registering schema", subject=subject, topic=topic, is_key=is_key)
        schema_id = self._client.register(subject, schema)
        self._cache.update(schema=schema, schema_id=schema_id, subject=subject)
        return schema_id

    def get_schema_id_or_register(self, schema, topic, is_key):
        schema_id = self._cache.get_schema_id(schema)
        if not schema_id:
            if not self._automatic_register:
                message = (
                    "You must enable 'schemas.automatic.register.enabled' to "
                    "automatically register schemas at runtime"
                )
                raise RuntimeError(message)
            logger.info(
                "Schema not found, continuing with registration", schema_name=schema.fullname,
                topic=topic, is_key=is_key
            )
            schema_id = self.register_schema(schema=schema, topic=topic, is_key=is_key)
        return schema_id

    def test_compatibility(self, schema, subject=None, topic=None, is_key=False):
        logger.info(
            "Testing compatibility for schema", subject=subject, schema_name=schema.fullname,
            topic=topic, is_key=is_key
        )
        try:
            *_, subject = self.get_latest_schema(
                schema=schema, subject=subject, topic=topic, is_key=is_key
            )
        except exceptions.SchemaNotFound:
            return True
        return self._client.test_compatibility(subject=subject, avro_schema=schema)


schema_registry = SchemaRegistryClient()
