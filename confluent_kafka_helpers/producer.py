import atexit
import math
import socket
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

import structlog
from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer
from opentelemetry.trace import SpanKind

from confluent_kafka_helpers.callbacks import (
    default_error_cb,
    default_on_delivery_cb,
    default_stats_cb,
    get_callback,
)
from confluent_kafka_helpers.context import get_propagated_headers
from confluent_kafka_helpers.exceptions import KafkaDeliveryError
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry, SchemaNotFound
from confluent_kafka_helpers.tracing import attributes as attrs
from confluent_kafka_helpers.tracing import datadog, tracer

logger = structlog.get_logger(__name__)


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """

    pass


class DeliveryScope:
    """Track all calls on one producer, including ordinary fallback publications.

    A producer must not be shared with unrelated work during a scope. Join worker
    threads before leaving it. Nested scopes on the same producer are rejected.
    """

    def __init__(self, producer: "AvroProducer", timeout: float):
        self._producer = producer
        self._deadline = time.monotonic() + timeout
        self._pending = 0
        self._failure: Exception | None = None
        self._lock = threading.Lock()

    def _check(self):
        with self._lock:
            if self._failure is not None:
                raise self._failure
            if time.monotonic() >= self._deadline:
                self._failure = TimeoutError("Kafka batch delivery timed out")
                raise self._failure

    def _produce(self, args):
        self._check()
        args = dict(args)
        callback = args.pop("callback", None)
        callback = args.pop("on_delivery", None) or callback
        with self._lock:
            self._pending += 1

        def delivered(error, message):
            try:
                try:
                    self._producer._delivery_callback(error, message)
                finally:
                    if callback is not None:
                        callback(error, message)
                if error:
                    raise KafkaDeliveryError(str(error), message)
            except Exception as failure:
                with self._lock:
                    if self._failure is None:
                        self._failure = failure
            finally:
                with self._lock:
                    self._pending -= 1

        try:
            while True:
                self._check()
                try:
                    self._producer._produce_once(**args, on_delivery=delivered)
                    return
                except BufferError:
                    self._producer.poll(max(0, min(self._deadline - time.monotonic(), 0.1)))
        except Exception as failure:
            with self._lock:
                self._pending -= 1
                if self._failure is None:
                    self._failure = failure
            raise

    def produce_many(self, records: Iterable[dict[str, Any]]) -> None:
        try:
            self._producer.produce_many(records)
        except Exception as failure:
            with self._lock:
                if self._failure is None:
                    self._failure = failure
            raise

    def _confirm(self):
        with self._lock:
            if self._failure is not None:
                raise self._failure
            if not self._pending:
                return
        self._check()
        outstanding = self._producer.flush(max(0, self._deadline - time.monotonic()))
        with self._lock:
            if self._failure is not None:
                raise self._failure
            if outstanding or self._pending:
                raise TimeoutError("Kafka batch still has unconfirmed output")


class AvroProducer(ConfluentAvroProducer):
    DEFAULT_CONFIG = {
        "client.id": socket.gethostname(),
        "log.connection.close": False,
        "enable.idempotence": True,
        "max.in.flight": 1,
        "linger.ms": 1000,
    }

    def __init__(
        self,
        config,
        value_serializer=None,
        schema_registry=AvroSchemaRegistry,
        get_callback=get_callback,
        **kwargs,
    ):
        config = {**self.DEFAULT_CONFIG, **config}
        config["on_delivery"] = get_callback(
            config.pop("on_delivery", None), default_on_delivery_cb
        )
        self._delivery_callback = config["on_delivery"]
        self._active_delivery_scope: DeliveryScope | None = None
        config["error_cb"] = get_callback(config.pop("error_cb", None), default_error_cb)
        config["stats_cb"] = get_callback(config.pop("stats_cb", None), default_stats_cb)

        schema_registry_url = config["schema.registry.url"]
        self.schema_registry = schema_registry(schema_registry_url)
        self.value_serializer = config.pop("value_serializer", value_serializer)

        self.bootstrap_servers = config["bootstrap.servers"]
        self.client_id = config["client.id"]

        topics = config.pop("topics")
        self.topic_schemas = self._get_topic_schemas(topics)

        # use the first topic as default
        default_topic_schema = next(iter(self.topic_schemas.values()))
        self.default_topic, *_ = default_topic_schema

        logger.info("Initializing producer", config=config)
        atexit.register(self._close)

        super().__init__(config, logger=logger, **kwargs)

    def _close(self):
        logger.info("Flushing producer")
        super().flush()

    def _get_subject_names(self, topic):
        """
        Get subject names for given topic.
        """
        key_subject_name = f"{topic}-key"
        value_subject_name = f"{topic}-value"
        return key_subject_name, value_subject_name

    def _get_topic_schemas(self, topics):
        """
        Get schemas for all topics.
        """
        topic_schemas = {}
        for topic in topics:
            key_name, value_name = self._get_subject_names(topic)
            try:
                key_schema = self.schema_registry.get_latest_schema(key_name)
            except SchemaNotFound:
                # topics that are used for only pub/sub will probably not
                # have a key set on the messages.
                #
                # on these topics we should not require a key schema.
                key_schema = None
            value_schema = self.schema_registry.get_latest_schema(value_name)
            topic_schemas[topic] = (topic, key_schema, value_schema)

        return topic_schemas

    @property
    def in_delivery_scope(self) -> bool:
        return self._active_delivery_scope is not None

    def produce(
        self, value, key=None, topic=None, headers=None, *, source: Message | None = None, **kwargs
    ):
        args = dict(value=value, key=key, topic=topic, headers=headers, source=source, **kwargs)
        if self._active_delivery_scope is not None:
            return self._active_delivery_scope._produce(args)
        return self._produce_once(**args)

    def _produce_once(
        self, value, key=None, topic=None, headers=None, *, source: Message | None = None, **kwargs
    ):
        span_options: dict[str, Any] = {}
        if source is None:
            propagated_headers = get_propagated_headers()
        else:
            propagated_headers = source.propagated_headers
            span_options["context"] = tracer.extract_headers(headers=source._meta.headers)
        logger.debug("Retrieved propagated headers", headers=propagated_headers)
        headers = {**propagated_headers, **(headers or {})}

        topic = topic or self.default_topic
        try:
            _, key_schema, value_schema = self.topic_schemas[topic]
        except KeyError:
            raise TopicNotRegistered(f"Topic {topic} is not registered")

        with tracer.start_span(
            name="kafka.produce", kind=SpanKind.PRODUCER, resource_name=topic, **span_options
        ) as span:
            if self.value_serializer:
                value = self.value_serializer(value)

            message_class = value.get("class") if isinstance(value, dict) else None
            resource_name = f"{topic}:{message_class}" if message_class else topic
            span.set_attribute(datadog.RESOURCE_NAME, resource_name)

            logger.info("Producing message", topic=topic, key=key, value=value, headers=headers)
            tracer.inject_headers(headers=headers)

            if message_class:
                span.set_attribute("message.class", message_class)

            span.set_attribute(
                attrs.MESSAGING_OPERATION_NAME,
                attrs.MESSAGING_OPERATION_NAME_VALUE_PRODUCE,
            )
            span.set_attribute(
                attrs.MESSAGING_OPERATION_TYPE,
                attrs.MESSAGING_OPERATION_TYPE_VALUE_PUBLISH,
            )
            span.set_attribute(attrs.MESSAGING_DESTINATION_NAME, topic)
            span.set_attribute(attrs.MESSAGING_CLIENT_ID, self.client_id)

            is_tombstone = key is not None and value is None
            span.set_attribute(attrs.MESSAGING_KAFKA_MESSAGE_TOMBSTONE, is_tombstone)

            if key:
                span.set_attribute(attrs.MESSAGING_KAFKA_MESSAGE_KEY, key)

            for header_key, header_value in propagated_headers.items():
                span.set_attribute(f"{attrs.PROPAGATED_HEADER_PREFIX}{header_key}", header_value)
            if correlation_id := propagated_headers.get(attrs.CORRELATION_ID_HEADER):
                span.set_attribute(attrs.MESSAGING_MESSAGE_CONVERSATION_ID, correlation_id)

            server_address, *server_port = self.bootstrap_servers.split(":")
            span.set_attribute(attrs.SERVER_ADDRESS, server_address)
            if server_port:
                span.set_attribute(attrs.SERVER_PORT, server_port[0])

            span.set_attribute(
                attrs.PRODUCER_SERVICE_NAME,
                datadog.get_datadog_service_name(),
            )

            super().produce(
                topic=topic,
                key=key,
                value=value,
                key_schema=key_schema,
                value_schema=value_schema,
                headers=headers,
                **kwargs,
            )

    @contextmanager
    def delivery_scope(self, timeout: float = 30.0) -> Iterator[DeliveryScope]:
        """Confirm all output from this producer before successful scope exit."""
        if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("delivery timeout must be finite and positive")
        if self.in_delivery_scope:
            raise RuntimeError("Nested delivery scopes on one producer are not supported")
        delivery = DeliveryScope(self, timeout)
        self._active_delivery_scope = delivery
        try:
            yield delivery
            delivery._confirm()
        finally:
            self._active_delivery_scope = None

    def produce_many(self, records: Iterable[dict[str, Any]], *, timeout: float = 30.0) -> None:
        """Queue records in order; use a delivery scope to confirm them before commit."""
        if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout < 0:
            raise ValueError("timeout must be finite and nonnegative")
        deadline = time.monotonic() + timeout
        for record in records:
            while True:
                try:
                    self.produce(**record)
                    break
                except BufferError as error:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Kafka producer queue remained full") from error
                    self.poll(min(remaining, 0.1))

    def flush(self, *args, **kwargs):
        with tracer.start_span(name="kafka.flush", kind=SpanKind.PRODUCER):
            logger.info("Flushing producer")
            return super().flush(*args, **kwargs)

    def poll(self, *args, **kwargs):
        return super().poll(*args, **kwargs)
