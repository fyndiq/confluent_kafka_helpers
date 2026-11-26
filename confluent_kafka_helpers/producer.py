from __future__ import annotations

import atexit
import socket
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator

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
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry, SchemaNotFound
from confluent_kafka_helpers.tracing import attributes as attrs
from confluent_kafka_helpers.tracing import datadog, tracer
from confluent_kafka_helpers.utils import validate_timeout

if TYPE_CHECKING:
    from confluent_kafka_helpers.message import Message

logger = structlog.get_logger(__name__)


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """

    pass


class _DeliveryScope:
    def __init__(self, producer: AvroProducer, timeout: float):
        self._producer = producer
        self._timeout = timeout
        self._deadline: float | None = None
        self._pending = 0
        self._failure: Exception | None = None

    def produce_many(self, records: Iterable[dict[str, Any]]) -> None:
        try:
            self._producer._enqueue(self._track(records), self._remaining)
        except Exception as error:
            if self._failure is None:
                self._failure = error
            raise

    def _remaining(self) -> float:
        if self._failure is not None:
            raise self._failure
        now = time.monotonic()
        if self._deadline is None:
            self._deadline = now + self._timeout
        remaining = self._deadline - now
        if remaining <= 0:
            raise TimeoutError("Kafka batch delivery timed out")
        return remaining

    def _track(self, records: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        for record in records:
            self._remaining()
            args = dict(record)
            callback = args.pop("callback", None)
            callback = args.pop("on_delivery", None) or callback
            self._pending += 1
            yield {**args, "on_delivery": partial(self._delivered, callback)}

    def _delivered(self, callback, error, message):
        try:
            try:
                self._producer._delivery_callback(error, message)
            finally:
                if callback is not None:
                    callback(error, message)
            if error:
                raise KafkaDeliveryError(str(error), message)
        except Exception as failure:
            if self._failure is None:
                self._failure = failure
        finally:
            self._pending -= 1

    def _confirm(self) -> None:
        if self._failure is not None:
            raise self._failure
        if not self._pending:
            return
        outstanding = self._producer.flush(self._remaining())
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
    ) -> None:
        config = {**self.DEFAULT_CONFIG, **config}
        config["on_delivery"] = get_callback(
            config.pop("on_delivery", None), default_on_delivery_cb
        )
        self._delivery_callback = config["on_delivery"]
        self._delivery: ContextVar[_DeliveryScope | None] = ContextVar(
            "kafka_delivery_scope", default=None
        )
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

    def produce(
        self, value, key=None, topic=None, headers=None, *, source: Message | None = None, **kwargs
    ):
        if delivery := self._delivery.get():
            delivery.produce_many(
                [dict(value=value, key=key, topic=topic, headers=headers, source=source, **kwargs)]
            )
        else:
            self._produce(value, key, topic, headers, source=source, **kwargs)

    def _produce(
        self, value, key=None, topic=None, headers=None, *, source: Message | None = None, **kwargs
    ):
        span_options: dict[str, Any] = {}
        if source is None:
            propagated_headers = get_propagated_headers()
        else:
            propagated_headers = source.propagated_headers
            span_options["context"] = source.trace_context
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
    def delivery_scope(self, timeout: float = 30.0) -> Iterator[AvroProducer]:
        """Confirm this producer's output on normal exit using one shared deadline."""
        validate_timeout(timeout, allow_zero=False)
        if self._delivery.get() is not None:
            raise RuntimeError("Cannot nest delivery scopes on the same producer")
        delivery = _DeliveryScope(self, timeout)
        token = self._delivery.set(delivery)
        try:
            yield self
            delivery._confirm()
        finally:
            self._delivery.reset(token)

    def produce_many(self, records: Iterable[dict[str, Any]], *, timeout: float = 30.0) -> None:
        """Enqueue records in order, allowing callbacks to free a full producer queue."""
        validate_timeout(timeout)
        if delivery := self._delivery.get():
            delivery.produce_many(records)
        else:
            deadline = time.monotonic() + timeout
            self._enqueue(records, lambda: deadline - time.monotonic())

    def _enqueue(
        self,
        records: Iterable[dict[str, Any]],
        remaining: Callable[[], float],
    ) -> None:
        for record in records:
            while True:
                try:
                    self._produce(**record)
                    break
                except BufferError as error:
                    wait = remaining()
                    if wait <= 0:
                        raise TimeoutError("Kafka producer queue remained full") from error
                self.poll(min(wait, 0.1))
                remaining()

    def finish_producing(self, *, flush: bool) -> None:
        """Serve callbacks and optionally flush outside a managed delivery scope."""
        if self._delivery.get() is None:
            self.poll(0)
            if flush:
                self.flush()

    def flush(self, *args, **kwargs):
        with tracer.start_span(name="kafka.flush", kind=SpanKind.PRODUCER):
            logger.info("Flushing producer")
            return super().flush(*args, **kwargs)

    def poll(self, *args, **kwargs):
        return super().poll(*args, **kwargs)
