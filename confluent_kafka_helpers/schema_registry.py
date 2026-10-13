import json
import time
from functools import lru_cache
from typing import Any

import structlog
from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.error import ClientError
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from opentelemetry.trace import SpanKind

from confluent_kafka_helpers.tracing import tracer

logger = structlog.get_logger(__name__)

SCHEMA_REGISTRY_PREFIX = "schema.registry."
RETRY_TIMEOUT_KEY = "retry.timeout"

# Producers can sit on a request path, so they give up quickly. Consumers block on the
# lookup anyway and wait long enough to ride out a registry restart.
DEFAULT_RETRY_TIMEOUT = 10.0
CONSUMER_RETRY_TIMEOUT = 120.0

INITIAL_BACKOFF = 0.5
MAX_BACKOFF = 10.0
MAX_BODY_LENGTH = 500


class SchemaNotFound(Exception):
    pass


class SchemaRegistryClient(CachedSchemaRegistryClient):
    """
    Schema registry client that retries transient registry failures.

    confluent-kafka's legacy client sends HTTPS requests through urllib3 whenever no
    `ssl.key.password` is set (its `_is_key_password_provided` flag is inverted). When that
    response is not JSON it returns `response.content`, which a urllib3 response does not have,
    so the caller gets an AttributeError and the registry's status and body are lost.

    Responses with a 5xx status or a non-JSON body are retried with exponential backoff until
    `retry_timeout` seconds have passed, then raised as a `ClientError` carrying the status and
    body. The Avro deserializer turns that into a `SerializerError`. The timeout can also be set
    with the `retry.timeout` key (`schema.registry.retry.timeout` in consumer/producer config).
    """

    def __init__(self, url, *args, retry_timeout: float = DEFAULT_RETRY_TIMEOUT, **kwargs):
        if isinstance(url, dict):
            url = dict(url)
            retry_timeout = float(url.pop(RETRY_TIMEOUT_KEY, retry_timeout))
        super().__init__(url, *args, **kwargs)
        self.retry_timeout = retry_timeout

    def _send_request(
        self, url: str, method: str = "GET", body: Any = None, headers: dict | None = None
    ) -> tuple[Any, int]:
        path = url.removeprefix(self.url)
        deadline = time.monotonic() + self.retry_timeout
        backoff = INITIAL_BACKOFF
        while True:
            result, status = self._send_request_once(url, method, body, headers or {})
            if not _is_transient(result, status):
                return result, status

            if time.monotonic() + backoff > deadline:
                message = f"Schema registry {method} {path} failed: HTTP {status}"
                raise ClientError(f"{message}: {_format_body(result)}", http_code=status)

            logger.warning(
                "Schema registry request failed, retrying",
                method=method,
                path=path,
                status=status,
                body=_format_body(result),
                retry_in=backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

    def _send_request_once(
        self, url: str, method: str, body: Any, headers: dict
    ) -> tuple[Any, int]:
        if not (url.startswith("https") and self._is_key_password_provided):
            return super()._send_request(url, method=method, body=body, headers=headers)

        response = self._send_https_session_request(url, method, headers, body)
        try:
            return json.loads(response.data), response.status
        except ValueError:
            return response.data, response.status


def _is_transient(result: Any, status: int) -> bool:
    # Both request paths return the raw body as bytes when it is not JSON.
    return status >= 500 or isinstance(result, bytes)


def _format_body(result: Any) -> str:
    if isinstance(result, bytes):
        result = result.decode(errors="replace")
    return repr(result)[:MAX_BODY_LENGTH]


def split_schema_registry_config(config: dict) -> tuple[dict, dict]:
    """
    Split `schema.registry.*` settings from Kafka client settings.

    Mirrors confluent's AvroConsumer/AvroProducer, which refuse an explicit `schema_registry`
    alongside `schema.registry.*` settings and do this split themselves otherwise.
    """
    registry_config = {
        key.removeprefix(SCHEMA_REGISTRY_PREFIX): value
        for key, value in config.items()
        if key.startswith(SCHEMA_REGISTRY_PREFIX)
    }
    if registry_config.get("basic.auth.credentials.source") == "SASL_INHERIT":
        # Fallback to plural 'mechanisms' for backward compatibility
        registry_config["sasl.mechanism"] = config.get(
            "sasl.mechanism", config.get("sasl.mechanisms", "")
        )
        registry_config["sasl.username"] = config.get("sasl.username", "")
        registry_config["sasl.password"] = config.get("sasl.password", "")

    client_config = {
        key: value for key, value in config.items() if not key.startswith(SCHEMA_REGISTRY_PREFIX)
    }
    return registry_config, client_config


class AvroSchemaRegistry:
    def __init__(
        self, schema_registry_url, client=SchemaRegistryClient, serializer=MessageSerializer
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
