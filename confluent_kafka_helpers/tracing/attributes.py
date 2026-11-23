"""
OpenTelemetry messaging semantic conventions, version 1.27.0.

The Semantic Conventions define a common set of (semantic) attributes which provide meaning to data
when collecting, producing and consuming it. The Semantic Conventions specify among other things
span names and kind, metric instruments and units as well as attribute names, types, meaning and
valid values.

References:
 - https://github.com/open-telemetry/semantic-conventions/blob/v1.27.0/docs/messaging/messaging-spans.md#messaging-attributes  # noqa
 - https://github.com/open-telemetry/semantic-conventions/blob/v1.27.0/docs/messaging/kafka.md
"""

# Keep the emitted schema stable across OpenTelemetry API upgrades.
SCHEMA_URL = "https://opentelemetry.io/schemas/1.27.0"

MESSAGING_SYSTEM = "messaging.system"
MESSAGING_SYSTEM_VALUE_KAFKA = "kafka"

SERVER_ADDRESS = "server.address"
SERVER_PORT = "server.port"

MESSAGING_OPERATION_NAME = "messaging.operation.name"
MESSAGING_OPERATION_NAME_VALUE_PRODUCE = "produce"
MESSAGING_OPERATION_NAME_VALUE_CONSUME = "consume"

MESSAGING_OPERATION_TYPE = "messaging.operation.type"
MESSAGING_OPERATION_TYPE_VALUE_CREATE = "create"
MESSAGING_OPERATION_TYPE_VALUE_PROCESS = "process"
MESSAGING_OPERATION_TYPE_VALUE_PUBLISH = "publish"
MESSAGING_OPERATION_TYPE_VALUE_RECEIVE = "receive"

MESSAGING_CLIENT_ID = "messaging.client.id"
MESSAGING_CONSUMER_GROUP_NAME = "messaging.consumer.group.name"
MESSAGING_DESTINATION_NAME = "messaging.destination.name"
MESSAGING_DESTINATION_PARTITION_ID = "messaging.destination.partition.id"
MESSAGING_KAFKA_MESSAGE_KEY = "messaging.kafka.message.key"
MESSAGING_KAFKA_MESSAGE_OFFSET = "messaging.kafka.message.offset"
MESSAGING_KAFKA_MESSAGE_TOMBSTONE = "messaging.kafka.message.tombstone"
MESSAGING_MESSAGE_CONVERSATION_ID = "messaging.message.conversation_id"

# custom attributes not part of the semantic ceonvention
PRODUCER_SERVICE_NAME = "confluent_kafka_helpers.producer.service.name"
PROPAGATED_HEADER_PREFIX = "confluent_kafka_helpers.header."
CORRELATION_ID_HEADER = "correlation_id"
