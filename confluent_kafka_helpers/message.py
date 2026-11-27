import datetime
from contextlib import contextmanager
from typing import Iterator

from opentelemetry.trace import Span, SpanKind

from confluent_kafka_helpers.context import propagated_headers
from confluent_kafka_helpers.tracing import attributes as attrs
from confluent_kafka_helpers.tracing import tracer
from confluent_kafka_helpers.tracing.span_attributes import set_message_span_attributes


def kafka_timestamp_to_datetime(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp / 1000.0) if timestamp is not None else None


def extract_timestamp_from_message(kafka_message):
    timestamp_type, timestamp = kafka_message.timestamp()
    if timestamp_type == 0 or timestamp <= 0:
        timestamp = None
    return timestamp


def decode_kafka_headers(headers):
    if headers and isinstance(headers, list):
        headers = {
            k: v.decode("utf-8") if isinstance(v, bytes) else v for k, v in dict(headers).items()
        }
    else:
        headers = {}
    return headers


class Message:
    __slots__ = ["value", "_raw", "_meta", "propagated_headers"]

    def __init__(self, kafka_message, propagate_header_keys=()):
        self.value = kafka_message.value()
        self._raw = kafka_message
        self._meta = MessageMetadata(kafka_message)
        self.propagated_headers = {
            k: v for k, v in self._meta.headers.items() if k in propagate_header_keys
        }

    def propagate(self):
        """Context manager: `producer.produce()` forwards this message's propagated headers
        inside the block."""
        return propagated_headers(self.propagated_headers)

    @contextmanager
    def process(self) -> Iterator[Span]:
        """Context manager for handling one message from a batch.

        Activates the incoming message trace as the parent of a processing span and forwards
        this message's propagated headers inside the block.
        """
        message_class = self.value.get("class") if isinstance(self.value, dict) else None
        resource_name = f"{self._meta.topic}:{message_class}" if message_class else self._meta.topic
        context = tracer.extract_headers(headers=self._meta.headers)
        with propagated_headers(self.propagated_headers):
            with tracer.start_span(
                name="kafka.process",
                kind=SpanKind.CONSUMER,
                resource_name=resource_name,
                context=context,
            ) as span:
                set_message_span_attributes(
                    span,
                    topic=self._meta.topic,
                    partition=self._meta.partition,
                    offset=self._meta.offset,
                    key=self._meta.key,
                    event_class=message_class,
                    operation_name=attrs.MESSAGING_OPERATION_NAME_VALUE_PROCESS,
                    operation_type=attrs.MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
                    propagated_headers=self.propagated_headers,
                    include_falsey_key=True,
                )
                yield span

    def __repr__(self):
        return f"Message(" f"value={self.value}, " f"_raw={self._raw}, " f"_meta={self._meta}" f")"

    def __bool__(self):
        return True if self.value else False

    def __eq__(self, other):
        return self._raw == other._raw

    def __hash__(self):
        return hash((str(self.value), self._meta.key, self._meta.offset))

    def __lt__(self, other):
        return hash(self) < hash(other)


class MessageMetadata:
    __slots__ = [
        "datetime",
        "headers",
        "key",
        "offset",
        "partition",
        "timestamp",
        "topic",
    ]

    def __init__(self, kafka_message):
        self.key = kafka_message.key()
        self.partition = kafka_message.partition()
        self.offset = kafka_message.offset()
        self.topic = kafka_message.topic()
        self.headers = decode_kafka_headers(kafka_message.headers())
        self.timestamp = extract_timestamp_from_message(kafka_message)
        self.datetime = kafka_timestamp_to_datetime(self.timestamp)

    def __repr__(self):
        return (
            f"MessageMetadata("
            f"key={self.key}, "
            f"partition={self.partition}, "
            f"offset={self.offset}, "
            f"topic={self.topic}, "
            f"headers={self.headers}, "
            f"timestamp={self.timestamp}, "
            f"datetime={self.datetime}"
            f")"
        )
