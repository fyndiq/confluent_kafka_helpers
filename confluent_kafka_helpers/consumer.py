import math
import socket
from functools import partial
from typing import Callable, Iterable

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from opentelemetry.trace import SpanKind

from confluent_kafka_helpers import is_shutdown_requested
from confluent_kafka_helpers.callbacks import (
    default_error_cb,
    default_on_commit_cb,
    default_stats_cb,
    get_callback,
)
from confluent_kafka_helpers.context import (
    clear_propagated_headers,
    propagated_headers_context,
    set_propagated_headers,
)
from confluent_kafka_helpers.exceptions import EndOfPartition, KafkaTransportError
from confluent_kafka_helpers.message import Message, decode_kafka_headers
from confluent_kafka_helpers.metrics import base_metric, statsd
from confluent_kafka_helpers.metrics.callbacks import on_commit_cb_metrics
from confluent_kafka_helpers.tracing import attributes as attrs
from confluent_kafka_helpers.tracing import tracer
from confluent_kafka_helpers.utils import (
    is_lost_assignment,
    is_retryable_commit_error,
    retry_exception,
)

logger = structlog.get_logger(__name__)

DEFAULT_PROPAGATED_HEADERS: frozenset[str] = frozenset({"origin_action_type", "action_started_at"})


def get_propagate_header_keys(configured_headers: Iterable[str] | None) -> set[str]:
    return set(DEFAULT_PROPAGATED_HEADERS) | set(configured_headers or [])


@retry_exception(exceptions=[KafkaTransportError])
def get_message(consumer, error_handler, timeout=0.1, stop_on_eof=False):
    message = consumer.poll(timeout=timeout)
    if message is None:
        return None

    if message.error():
        try:
            error_handler(message.error())
        except EndOfPartition:
            if stop_on_eof:
                raise
            else:
                return None

    return message


def default_error_handler(kafka_error):
    code = kafka_error.code()
    if code == KafkaError._PARTITION_EOF:
        raise EndOfPartition
    elif code == KafkaError._TRANSPORT:
        statsd.increment(f"{base_metric}.consumer.message.count.error")
        raise KafkaTransportError(kafka_error)
    else:
        statsd.increment(f"{base_metric}.consumer.message.count.error")
        raise KafkaException(kafka_error)


def is_kafka_transient_error(exc: KafkaException):
    if not exc.args:
        return False
    error = exc.args[0]
    if not isinstance(error, KafkaError):
        return False
    return is_retryable_commit_error(error)


class _AvroConsumer:
    DEFAULT_CONFIG = {
        "client.id": socket.gethostname(),
        "default.topic.config": {"auto.offset.reset": "earliest"},
        "enable.auto.commit": False,
        "fetch.wait.max.ms": 1000,
        "fetch.min.bytes": 10000,
        "log.connection.close": False,
        "log.thread.name": False,
    }

    def __init__(self, config, **kwargs) -> None:
        self.config = {**self.DEFAULT_CONFIG, **config}
        self.config["error_cb"] = get_callback(config.pop("error_cb", None), default_error_cb)
        self.config["stats_cb"] = get_callback(config.pop("stats_cb", None), default_stats_cb)
        self.config["on_commit"] = get_callback(config.pop("on_commit", None), default_on_commit_cb)
        self.topics = self._get_topics(self.config)

        self.client_id = self.config["client.id"]
        self.bootstrap_servers = self.config["bootstrap.servers"]
        self.group_id = self.config["group.id"]

        self.propagate_header_keys = get_propagate_header_keys(
            self.config.pop("headers.propagate", [])
        )

        logger.info("Initializing consumer", config=self.config)
        self.consumer = ConfluentAvroConsumer(self.config, logger=logger, **kwargs)
        self._subscribe()

        self._generator = self._message_generator()

    def __getattr__(self, name):
        return getattr(self.consumer, name)

    def _subscribe(self):
        self.consumer.subscribe(self.topics)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._generator)
        except EndOfPartition:
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # the only reason a consumer exits is when an
        # exception is raised.
        #
        # close down the consumer cleanly accordingly:
        #  - stops consuming
        #  - commit offsets (only on auto commit)
        #  - leave consumer group
        logger.info("Closing consumer")
        # close generator. otherwise, in case of exceptions, it won't be closed until garbage
        # collection occurs, which might lead to unexpected behaviour, eg. spans not closing before
        # sent to the tracing backend
        try:
            self._generator.close()
        finally:
            self.consumer.close()

    def _get_topics(self, config):
        topics = config.pop("topics", None)
        assert topics is not None, "You must subscribe to at least one topic"

        if not isinstance(topics, list):
            topics = [topics]

        return topics

    @property
    def is_auto_commit(self):
        return self.config.get("enable.auto.commit", True)


class AvroConsumer(_AvroConsumer):
    def __init__(
        self,
        config,
        get_message: Callable = get_message,
        error_handler: Callable = default_error_handler,
        **kwargs,
    ) -> None:
        stop_on_eof = config.pop("stop_on_eof", False)
        poll_timeout = config.pop("poll_timeout", 0.1)
        self.non_blocking = config.pop("non_blocking", False)
        super().__init__(config, **kwargs)
        self._get_message = partial(
            get_message,
            consumer=self.consumer,
            error_handler=error_handler,
            timeout=poll_timeout,
            stop_on_eof=stop_on_eof,
        )

    def _message_generator(self):
        while not is_shutdown_requested():
            message = self._get_message()
            if message is None:
                if self.non_blocking:
                    yield None
                continue

            statsd.increment(f"{base_metric}.consumer.message.count.total")

            value, resource_name = message.value(), message.topic()
            message_class = value.get("class") if isinstance(value, dict) else None
            resource_name = f"{message.topic()}:{message_class}" if message_class else resource_name

            headers = decode_kafka_headers(message.headers())
            propagated_headers = {
                k: v for k, v in headers.items() if k in self.propagate_header_keys
            }
            logger.debug("Setting propagated headers", headers=propagated_headers)
            set_propagated_headers(propagated_headers)

            context = tracer.extract_headers(headers=headers)
            with tracer.start_span(
                name="kafka.consume",
                kind=SpanKind.CONSUMER,
                resource_name=resource_name,
                context=context,
                links=tracer.extract_links(context=context),
            ) as span:
                message = Message(message, self.propagate_header_keys)

                if message_class:
                    span.set_attribute("message.class", message_class)

                span.set_attribute(
                    attrs.MESSAGING_OPERATION_NAME,
                    attrs.MESSAGING_OPERATION_NAME_VALUE_CONSUME,
                )
                span.set_attribute(
                    attrs.MESSAGING_OPERATION_TYPE,
                    attrs.MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
                )
                span.set_attribute(attrs.MESSAGING_CLIENT_ID, self.client_id)
                span.set_attribute(attrs.MESSAGING_DESTINATION_NAME, message._meta.topic)
                span.set_attribute(attrs.MESSAGING_CONSUMER_GROUP_NAME, self.group_id)
                span.set_attribute(
                    attrs.MESSAGING_DESTINATION_PARTITION_ID, message._meta.partition
                )
                span.set_attribute(attrs.MESSAGING_KAFKA_MESSAGE_OFFSET, message._meta.offset)

                if message._meta.key:
                    span.set_attribute(attrs.MESSAGING_KAFKA_MESSAGE_KEY, message._meta.key)

                for header_key, header_value in propagated_headers.items():
                    span.set_attribute(
                        f"{attrs.PROPAGATED_HEADER_PREFIX}{header_key}", header_value
                    )
                if correlation_id := propagated_headers.get(attrs.CORRELATION_ID_HEADER):
                    span.set_attribute(attrs.MESSAGING_MESSAGE_CONVERSATION_ID, correlation_id)

                server_address, *server_port = self.bootstrap_servers.split(":")
                span.set_attribute(attrs.SERVER_ADDRESS, server_address)
                if server_port:
                    span.set_attribute(attrs.SERVER_PORT, server_port[0])

                try:
                    yield message
                finally:
                    clear_propagated_headers()
        logger.info("Shutdown requested, exiting consumer loop")

    @retry_exception(exceptions={KafkaException}, condition=is_kafka_transient_error, delay=0.1)
    def commit(self, *args, **kwargs):
        try:
            self.consumer.commit(*args, **kwargs)
        except KafkaException as exc:
            if not exc.args or not isinstance(exc.args[0], KafkaError):
                raise
            error = exc.args[0]
            on_commit_cb_metrics(error, None)
            # Replaying these offsets after ownership changes could commit stale work.
            if is_lost_assignment(error):
                logger.warning(
                    "Kafka assignment lost; offsets were not committed", error=str(error)
                )
                return
            raise


def validate_batch_options(batch_size, timeout):
    if type(batch_size) is not int or not 1 <= batch_size <= 1_000_000:
        raise ValueError("batch_size must be an integer from 1 to 1,000,000")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("timeout must be finite and nonnegative")
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be finite and nonnegative")


class AvroBatchConsumer(_AvroConsumer):
    """Consume ordered batches and commit their processed offsets explicitly."""

    def __init__(
        self,
        config,
        batch_size=100,
        timeout=0.1,
        *,
        error_handler: Callable = default_error_handler,
        **kwargs,
    ) -> None:
        validate_batch_options(batch_size, timeout)
        if "get_message" in kwargs:
            raise TypeError("AvroBatchConsumer does not support get_message")
        config = dict(config)
        for setting in ("poll_timeout", "non_blocking"):
            if setting in config:
                raise ValueError(f"AvroBatchConsumer does not support {setting}")
        for setting in ("enable.auto.commit", "enable.auto.offset.store"):
            if setting in config and str(config[setting]).lower() not in {"false", "0"}:
                raise ValueError(f"Batch consumption requires {setting}=false")
            config[setting] = False

        self.batch_size = batch_size
        self.timeout = timeout
        self.stop_on_eof = config.pop("stop_on_eof", False)
        self._error_handler = error_handler
        self._eof_partitions: set[tuple[str, int]] = set()
        if self.stop_on_eof:
            config["enable.partition.eof"] = True
        super().__init__(config, **kwargs)

    def _subscribe(self):
        if self.stop_on_eof:
            self.consumer.subscribe(
                self.topics,
                on_assign=self._reset_eof,
                on_revoke=self._reset_eof,
                on_lost=self._reset_eof,
            )
        else:
            super()._subscribe()

    def _reset_eof(self, consumer, partitions):
        self._eof_partitions.difference_update((p.topic, p.partition) for p in partitions)

    def _message_generator(self):
        while not is_shutdown_requested():
            messages = []
            for raw in self.consumer.consume(num_messages=self.batch_size, timeout=self.timeout):
                partition = (raw.topic(), raw.partition())
                if error := raw.error():
                    if error.code() == KafkaError._PARTITION_EOF:
                        self._eof_partitions.add(partition)
                        continue
                    self._error_handler(error)
                    continue
                self._eof_partitions.discard(partition)
                try:
                    if raw.value() is not None:
                        raw.set_value(self.consumer._serializer.decode_message(raw.value()))
                    if raw.key() is not None:
                        raw.set_key(
                            self.consumer._serializer.decode_message(raw.key(), is_key=True)
                        )
                except SerializerError as error:
                    raise SerializerError(
                        f"Avro decoding failed at {raw.topic()} [{raw.partition()}] "
                        f"offset {raw.offset()}"
                    ) from error
                messages.append(Message(raw, self.propagate_header_keys))

            if messages:
                statsd.increment(f"{base_metric}.consumer.message.count.total", len(messages))
                links = [
                    link
                    for message in messages
                    for link in tracer.extract_links(tracer.extract_headers(message._meta.headers))
                ]
                with (
                    propagated_headers_context({}),
                    tracer.start_span(
                        name="kafka.consume_batch", kind=SpanKind.CONSUMER, links=links
                    ) as span,
                ):
                    span.set_attribute(attrs.MESSAGING_CLIENT_ID, self.client_id)
                    span.set_attribute(attrs.MESSAGING_CONSUMER_GROUP_NAME, self.group_id)
                    yield messages

            if self.stop_on_eof:
                assigned = {(p.topic, p.partition) for p in self.consumer.assignment()}
                if assigned and assigned <= self._eof_partitions:
                    return

    def commit_batch(self, messages: Iterable[Message]) -> None:
        offsets: dict[tuple[str, int], int] = {}
        for message in messages:
            meta = message._meta
            partition = (meta.topic, meta.partition)
            offsets[partition] = max(offsets.get(partition, 0), meta.offset + 1)
        if not offsets:
            return

        partitions = [TopicPartition(*partition, offset) for partition, offset in offsets.items()]
        try:
            result = self.consumer.commit(offsets=partitions, asynchronous=False)
        except KafkaException as error:
            if error.args and isinstance(error.args[0], KafkaError):
                on_commit_cb_metrics(error.args[0], None)
            raise
        for committed_partition in result:
            if committed_partition.error is not None:
                on_commit_cb_metrics(committed_partition.error, result)
                raise KafkaException(committed_partition.error)


class AvroLazyConsumer(ConfluentAvroConsumer):
    """
    By default the Confluent AvroConsumer decode all messages in the partition.

    This consumer uses a lazy approach, it doesn't decode the messages, just
    provide the methods so we can do it manually.

    We use this approach, because we want to check the key messages before
    decoding the message, this will avoid performance issues.
    """

    def poll(self, timeout=None):
        if timeout is None:
            timeout = -1

        # We use the Consumer.poll because we want to avoid the
        # ConfluentAvroConsumer.poll, the later is doing the decode
        # of all the messages and we want to have a lazy approach
        message = Consumer.poll(self, timeout)
        return message

    def decode_message(self, message):
        if not message.error():
            if message.value() is not None:
                decoded_value = self._serializer.decode_message(message.value())
                message.set_value(decoded_value)

            if message.key() is not None:
                decoded_key = self._serializer.decode_message(message.key())
                message.set_key(decoded_key)
        return message
