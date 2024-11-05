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

from opentelemetry.semconv._incubating.attributes import messaging_attributes, server_attributes
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MessagingOperationTypeValues,
    MessagingSystemValues,
)

MESSAGING_SYSTEM = messaging_attributes.MESSAGING_SYSTEM
MESSAGING_SYSTEM_VALUE_KAFKA = MessagingSystemValues.KAFKA.value

SERVER_ADDRESS = server_attributes.SERVER_ADDRESS
SERVER_PORT = server_attributes.SERVER_PORT

MESSAGING_OPERATION_NAME = messaging_attributes.MESSAGING_OPERATION_NAME
MESSAGING_OPERATION_NAME_VALUE_PRODUCE = "produce"
MESSAGING_OPERATION_NAME_VALUE_CONSUME = "consume"

MESSAGING_OPERATION_TYPE = messaging_attributes.MESSAGING_OPERATION_TYPE
MESSAGING_OPERATION_TYPE_VALUE_CREATE = MessagingOperationTypeValues.CREATE.value
MESSAGING_OPERATION_TYPE_VALUE_PROCESS = MessagingOperationTypeValues.PROCESS.value
MESSAGING_OPERATION_TYPE_VALUE_PUBLISH = MessagingOperationTypeValues.PUBLISH.value
MESSAGING_OPERATION_TYPE_VALUE_RECEIVE = MessagingOperationTypeValues.RECEIVE.value

MESSAGING_CLIENT_ID = messaging_attributes.MESSAGING_CLIENT_ID
MESSAGING_CONSUMER_GROUP_NAME = messaging_attributes.MESSAGING_CONSUMER_GROUP_NAME
MESSAGING_DESTINATION_NAME = messaging_attributes.MESSAGING_DESTINATION_NAME
MESSAGING_DESTINATION_PARTITION_ID = messaging_attributes.MESSAGING_DESTINATION_PARTITION_ID
MESSAGING_KAFKA_MESSAGE_KEY = messaging_attributes.MESSAGING_KAFKA_MESSAGE_KEY
MESSAGING_KAFKA_MESSAGE_OFFSET = messaging_attributes.MESSAGING_KAFKA_MESSAGE_OFFSET
MESSAGING_KAFKA_MESSAGE_TOMBSTONE = messaging_attributes.MESSAGING_KAFKA_MESSAGE_TOMBSTONE

# custom attributes not part of the semantic ceonvention
MESSAGING_PRODUCER_SERVICE_NAME = "messaging.producer.service.name"
