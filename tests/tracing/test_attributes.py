import ast
import inspect

from confluent_kafka_helpers.tracing import attributes

# Messaging semantic conventions v1.27.0, as the helper has emitted them since
# it adopted that version. A changed value changes every consumer's spans.
SEMCONV_V1_27_0 = {
    "MESSAGING_CLIENT_ID": "messaging.client.id",
    "MESSAGING_CONSUMER_GROUP_NAME": "messaging.consumer.group.name",
    "MESSAGING_DESTINATION_NAME": "messaging.destination.name",
    "MESSAGING_DESTINATION_PARTITION_ID": "messaging.destination.partition.id",
    "MESSAGING_KAFKA_MESSAGE_KEY": "messaging.kafka.message.key",
    "MESSAGING_KAFKA_MESSAGE_OFFSET": "messaging.kafka.message.offset",
    "MESSAGING_KAFKA_MESSAGE_TOMBSTONE": "messaging.kafka.message.tombstone",
    "MESSAGING_MESSAGE_CONVERSATION_ID": "messaging.message.conversation_id",
    "MESSAGING_OPERATION_NAME": "messaging.operation.name",
    "MESSAGING_OPERATION_NAME_VALUE_CONSUME": "consume",
    "MESSAGING_OPERATION_NAME_VALUE_PRODUCE": "produce",
    "MESSAGING_OPERATION_TYPE": "messaging.operation.type",
    "MESSAGING_OPERATION_TYPE_VALUE_CREATE": "create",
    "MESSAGING_OPERATION_TYPE_VALUE_PROCESS": "process",
    "MESSAGING_OPERATION_TYPE_VALUE_PUBLISH": "publish",
    "MESSAGING_OPERATION_TYPE_VALUE_RECEIVE": "receive",
    "MESSAGING_SYSTEM": "messaging.system",
    "MESSAGING_SYSTEM_VALUE_KAFKA": "kafka",
    "SERVER_ADDRESS": "server.address",
    "SERVER_PORT": "server.port",
}


def test_attributes_match_semconv_v1_27_0():
    assert {name: getattr(attributes, name) for name in SEMCONV_V1_27_0} == SEMCONV_V1_27_0


def test_attributes_do_not_import_incubating_semconv():
    tree = ast.parse(inspect.getsource(attributes))
    imported = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    imported += [
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    ]
    assert [name for name in imported if name and "_incubating" in name] == []
