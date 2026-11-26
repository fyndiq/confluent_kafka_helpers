import time
from functools import lru_cache

import requests  # type: ignore[import-untyped]
import structlog
import urllib3
from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from opentelemetry.trace import SpanKind

from confluent_kafka_helpers.tracing import tracer

logger = structlog.get_logger(__name__)


class SchemaNotFound(Exception):
    pass


# Connection-level errors that are safe to retry for schema registration.
# Registering an already registered schema is idempotent and returns the same id.
TRANSIENT_ERRORS = (
    urllib3.exceptions.ProtocolError,
    urllib3.exceptions.NewConnectionError,
    urllib3.exceptions.MaxRetryError,
    requests.exceptions.ConnectionError,
    ConnectionError,
)


class RetryingSchemaRegistryClient(CachedSchemaRegistryClient):
    """
    Schema registry client that retries registration on transient connection errors.

    urllib3 does not retry read errors for POST requests, so a stale keep-alive
    connection closed by the registry surfaces as a ProtocolError on the first
    register call after a pod starts.
    """

    max_attempts = 3
    backoff_seconds = 0.2

    def _with_retry(self, operation, method, subject, avro_schema):
        for attempt in range(1, self.max_attempts + 1):
            try:
                return method(subject, avro_schema)
            except TRANSIENT_ERRORS as exc:
                if attempt == self.max_attempts:
                    raise
                logger.warning(
                    "Transient schema registry error, retrying",
                    operation=operation,
                    subject=subject,
                    attempt=attempt,
                    max_attempts=self.max_attempts,
                    error=repr(exc),
                )
                time.sleep(self.backoff_seconds * attempt)

    def register(self, subject, avro_schema):
        return self._with_retry("register", super().register, subject, avro_schema)

    def check_registration(self, subject, avro_schema):
        return self._with_retry(
            "check_registration", super().check_registration, subject, avro_schema
        )


class AvroSchemaRegistry:
    def __init__(
        self, schema_registry_url, client=CachedSchemaRegistryClient, serializer=MessageSerializer
    ):
        self.client = client(url=schema_registry_url)
        self.serializer = serializer(self.client)

    def get_latest_schema(self, subject):
        with tracer.start_span(
            name="schema_registry.get_latest_schema",
            kind=SpanKind.CLIENT,
            service_name="schema-registry",
            resource_name=subject,
        ) as span:
            schema_id, schema, version = self.client.get_latest_schema(subject)
            span.set_attribute("schema_id", str(schema_id))
            span.set_attribute("version", str(version))
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
