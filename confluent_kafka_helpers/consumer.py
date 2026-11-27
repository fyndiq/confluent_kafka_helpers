import contextlib
import os
import socket
from functools import partial
from typing import Callable, Collection, Generator, Iterable, Iterator, Protocol

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka import Message as KafkaMessage
from confluent_kafka import TopicPartition
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from opentelemetry.context import Context
from opentelemetry.trace import Link, Span, SpanKind

from confluent_kafka_helpers import is_shutdown_requested
from confluent_kafka_helpers.callbacks import (
    default_error_cb,
    default_on_commit_cb,
    default_stats_cb,
    get_callback,
)
from confluent_kafka_helpers.context import (
    clear_propagated_headers,
    propagated_headers,
    set_propagated_headers,
)
from confluent_kafka_helpers.exceptions import EndOfPartition, KafkaTransportError
from confluent_kafka_helpers.message import Message, decode_kafka_headers
from confluent_kafka_helpers.metrics import base_metric, statsd
from confluent_kafka_helpers.metrics.callbacks import on_commit_cb_metrics
from confluent_kafka_helpers.tracing import attributes as attrs
from confluent_kafka_helpers.tracing import tracer
from confluent_kafka_helpers.tracing.span_attributes import (
    set_batch_consume_span_attributes,
    set_message_span_attributes,
)
from confluent_kafka_helpers.utils import (
    is_lost_assignment,
    is_retryable_commit_error,
    retry_exception,
    validate_timeout,
)

logger = structlog.get_logger(__name__)

DEFAULT_PROPAGATED_HEADERS: frozenset[str] = frozenset({"origin_action_type", "action_started_at"})
DEFAULT_BATCH_SPAN_LINK_COUNT_LIMIT = 128
OTEL_SPAN_LINK_COUNT_LIMIT = "OTEL_SPAN_LINK_COUNT_LIMIT"


def get_propagate_header_keys(configured_headers: Iterable[str] | None) -> set[str]:
    return set(DEFAULT_PROPAGATED_HEADERS) | set(configured_headers or [])


def _batch_span_link_count_limit() -> int:
    value = os.getenv(OTEL_SPAN_LINK_COUNT_LIMIT)
    if value is None:
        return DEFAULT_BATCH_SPAN_LINK_COUNT_LIMIT
    try:
        return max(0, int(value))
    except ValueError:
        return DEFAULT_BATCH_SPAN_LINK_COUNT_LIMIT


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


class _AvroDecoder(Protocol):
    def __call__(self, data: bytes, /, *, is_key: bool) -> object:
        """Decode wire bytes with the key or value reader schema."""


def _decode_batch_record(
    record: KafkaMessage, decode: _AvroDecoder, propagate_header_keys: Collection[str]
) -> Message:
    """Build a decoded message while preserving the original Kafka wire record."""
    message = Message(record, propagate_header_keys)
    try:
        if message.value is not None:
            message.value = decode(message.value, is_key=False)
        if message._meta.key is not None:
            message._meta.key = decode(message._meta.key, is_key=True)
    except SerializerError as exc:
        raise SerializerError(
            f"Message deserialization failed for message at {record.topic()} "
            f"[{record.partition()}] offset {record.offset()}: {exc}"
        ) from exc
    return message


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
        return topics if isinstance(topics, list) else [topics]

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
                with tracer.start_span(name="kafka.create_message"):
                    message = Message(message, self.propagate_header_keys)

                set_message_span_attributes(
                    span,
                    topic=message._meta.topic,
                    partition=message._meta.partition,
                    offset=message._meta.offset,
                    key=message._meta.key,
                    event_class=message_class,
                    operation_name=attrs.MESSAGING_OPERATION_NAME_VALUE_CONSUME,
                    operation_type=attrs.MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
                    client_id=self.client_id,
                    consumer_group=self.group_id,
                    propagated_headers=propagated_headers,
                    bootstrap_servers=self.bootstrap_servers,
                )

                try:
                    yield message
                finally:
                    clear_propagated_headers()
        logger.info("Shutdown requested, exiting consumer loop")

    @retry_exception(exceptions={KafkaException}, condition=is_kafka_transient_error, delay=0.1)
    def commit(self, *args, **kwargs):
        with tracer.start_span(name="kafka.commit", kind=SpanKind.CONSUMER):
            try:
                self.consumer.commit(*args, **kwargs)
            except KafkaException as exc:
                if not exc.args or not isinstance(exc.args[0], KafkaError):
                    raise
                error = exc.args[0]
                on_commit_cb_metrics(error, None)
                if is_lost_assignment(error):
                    logger.warning(
                        "Kafka assignment lost; offsets were not committed", error=str(error)
                    )
                    return
                raise


def validate_batch_options(batch_size, timeout):
    if type(batch_size) is not int or not 1 <= batch_size <= 1_000_000:
        raise ValueError("batch_size must be an integer from 1 to 1,000,000")
    validate_timeout(timeout)


class AvroBatchConsumer(_AvroConsumer):
    """Yield ordered batches; acknowledge processed partition prefixes explicitly."""

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
        for setting in ("poll_timeout", "non_blocking", "batch_max_size", "batch_max_wait"):
            if setting in config:
                raise ValueError(f"Use batch_size and timeout; batch consumer rejects {setting}")
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

    def _message_generator(self) -> Generator[list[Message], None, None]:
        while not is_shutdown_requested():
            records = self.consumer.consume(num_messages=self.batch_size, timeout=self.timeout)
            if is_shutdown_requested():
                if records:
                    logger.info(
                        "Shutdown requested after Kafka batch read; leaving records unprocessed",
                        records=len(records),
                    )
                break
            messages = []
            for record in records:
                partition = (record.topic(), record.partition())
                if error := record.error():
                    if error.code() == KafkaError._PARTITION_EOF:
                        self._eof_partitions.add(partition)
                        continue
                    self._error_handler(error)
                else:
                    self._eof_partitions.discard(partition)
                    messages.append(
                        _decode_batch_record(
                            record,
                            self.consumer._serializer.decode_message,
                            self.propagate_header_keys,
                        )
                    )
            if messages:
                with propagated_headers({}), self._batch_span(messages):
                    yield messages
            if self.stop_on_eof:
                assigned = {(p.topic, p.partition) for p in self.consumer.assignment()}
                if assigned and assigned <= self._eof_partitions:
                    return

    @contextlib.contextmanager
    def _batch_span(self, messages: list[Message]) -> Iterator[Span]:
        statsd.increment(  # type: ignore[attr-defined]
            f"{base_metric}.consumer.message.count.total", len(messages)
        )

        topics = sorted({m._meta.topic for m in messages})
        links: list[Link] = []
        link_limit = _batch_span_link_count_limit()
        for message in messages:
            if len(links) >= link_limit:
                break
            context = tracer.extract_headers(headers=message._meta.headers)
            for link in tracer.extract_links(context=context):
                links.append(link)
                if len(links) >= link_limit:
                    break
        # no single message's trace is the rightful parent: start a new trace, link them all
        with tracer.start_span(
            name="kafka.consume",
            kind=SpanKind.CONSUMER,
            resource_name=",".join(topics),
            context=Context(),
            links=links,
        ) as span:
            set_batch_consume_span_attributes(
                span,
                message_count=len(messages),
                topics=topics,
                client_id=self.client_id,
                consumer_group=self.group_id,
                bootstrap_servers=self.bootstrap_servers,
            )

            yield span

    def commit_batch(self, messages: Iterable[Message]) -> None:
        """Commit only successful prefixes per partition; failures require stop/replay."""
        offsets: dict[tuple[str, int], int] = {}
        for message in messages:
            meta = message._meta
            partition = (meta.topic, meta.partition)
            offsets[partition] = max(offsets.get(partition, 0), meta.offset + 1)
        if not offsets:
            return
        partitions = [TopicPartition(*partition, offset) for partition, offset in offsets.items()]
        with tracer.start_span(name="kafka.commit", kind=SpanKind.CONSUMER):
            try:
                result = self.consumer.commit(offsets=partitions, asynchronous=False)
            except KafkaException as exc:
                if exc.args and isinstance(exc.args[0], KafkaError):
                    on_commit_cb_metrics(exc.args[0], None)
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
