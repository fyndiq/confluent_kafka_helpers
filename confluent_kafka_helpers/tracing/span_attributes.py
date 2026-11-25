from collections.abc import Mapping, Sequence
from typing import Any

from opentelemetry.trace import Span
from opentelemetry.util.types import AttributeValue

from confluent_kafka_helpers.tracing import attributes as attrs


def _set_server_attributes(span: Span, bootstrap_servers: str | None) -> None:
    if not bootstrap_servers:
        return
    server_address, *server_port = bootstrap_servers.split(":")
    span.set_attribute(attrs.SERVER_ADDRESS, server_address)
    if server_port:
        span.set_attribute(attrs.SERVER_PORT, server_port[0])


def _set_propagated_header_attributes(
    span: Span, propagated_headers: Mapping[str, Any] | None
) -> None:
    if not propagated_headers:
        return
    for header_key, header_value in propagated_headers.items():
        span.set_attribute(f"{attrs.PROPAGATED_HEADER_PREFIX}{header_key}", header_value)
    if correlation_id := propagated_headers.get(attrs.CORRELATION_ID_HEADER):
        span.set_attribute(attrs.MESSAGING_MESSAGE_CONVERSATION_ID, correlation_id)


def set_message_span_attributes(
    span: Span,
    *,
    topic: str,
    partition: int,
    offset: int,
    key: AttributeValue | None,
    event_class: str | None,
    operation_name: str,
    operation_type: str,
    client_id: str | None = None,
    consumer_group: str | None = None,
    propagated_headers: Mapping[str, Any] | None = None,
    bootstrap_servers: str | None = None,
    include_falsey_key: bool = False,
) -> None:
    if event_class:
        span.set_attribute("message.class", event_class)

    span.set_attribute(attrs.MESSAGING_OPERATION_NAME, operation_name)
    span.set_attribute(attrs.MESSAGING_OPERATION_TYPE, operation_type)
    if client_id:
        span.set_attribute(attrs.MESSAGING_CLIENT_ID, client_id)
    span.set_attribute(attrs.MESSAGING_DESTINATION_NAME, topic)
    if consumer_group:
        span.set_attribute(attrs.MESSAGING_CONSUMER_GROUP_NAME, consumer_group)
    span.set_attribute(attrs.MESSAGING_DESTINATION_PARTITION_ID, partition)
    span.set_attribute(attrs.MESSAGING_KAFKA_MESSAGE_OFFSET, offset)

    if key is not None and (include_falsey_key or bool(key)):
        span.set_attribute(attrs.MESSAGING_KAFKA_MESSAGE_KEY, key)

    _set_propagated_header_attributes(span, propagated_headers)
    _set_server_attributes(span, bootstrap_servers)


def set_batch_consume_span_attributes(
    span: Span,
    *,
    message_count: int,
    topics: Sequence[str],
    client_id: str,
    consumer_group: str,
    bootstrap_servers: str,
) -> None:
    span.set_attribute(attrs.MESSAGING_BATCH_MESSAGE_COUNT, message_count)
    span.set_attribute(attrs.MESSAGING_OPERATION_NAME, attrs.MESSAGING_OPERATION_NAME_VALUE_CONSUME)
    span.set_attribute(attrs.MESSAGING_OPERATION_TYPE, attrs.MESSAGING_OPERATION_TYPE_VALUE_RECEIVE)
    span.set_attribute(attrs.MESSAGING_CLIENT_ID, client_id)
    span.set_attribute(attrs.MESSAGING_CONSUMER_GROUP_NAME, consumer_group)
    if len(topics) == 1:
        span.set_attribute(attrs.MESSAGING_DESTINATION_NAME, topics[0])

    _set_server_attributes(span, bootstrap_servers)
