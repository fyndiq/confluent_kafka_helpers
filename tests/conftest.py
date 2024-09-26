from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer

from confluent_kafka_helpers import consumer

from tests import config


@pytest.fixture
def confluent_message():
    class PollReturnMock:
        value = MagicMock()
        value.return_value = b'foobar'
        error = MagicMock()
        error.return_value = None
        key = MagicMock()
        key.return_value = 1
        offset = MagicMock()
        offset.return_value = 0
        partition = MagicMock()
        partition.return_value = 1
        topic = MagicMock()
        topic.return_value = "test"
        timestamp = MagicMock()
        timestamp.return_value = (1, -1)
        headers = MagicMock(return_value=[('foo', b'bar')])

    return PollReturnMock()


@pytest.fixture
def confluent_avro_consumer(confluent_message):
    class ConfluentAvroConsumerMock(MagicMock):
        subscribe = MagicMock()
        poll = MagicMock(name='poll', side_effect=[confluent_message, StopIteration])
        close = MagicMock()
        get_watermark_offsets = MagicMock(name='watermark_test', return_value=[1, 1])

    return ConfluentAvroConsumerMock(spec=ConfluentAvroConsumer, name='ConfluentAvroConsumerMock')


@pytest.fixture
def avro_consumer(confluent_avro_consumer):
    consumer_config = config.Config.KAFKA_CONSUMER_CONFIG
    consumer_config["client.id"] = "<client-id>"
    with ExitStack() as stack:
        stack.enter_context(
            patch('confluent_kafka_helpers.consumer.ConfluentAvroConsumer', confluent_avro_consumer)
        )
        yield consumer.AvroConsumer(consumer_config)


@pytest.fixture
def avro_schema_registry():
    schema_registry = MagicMock()
    with ExitStack() as stack:
        stack.enter_context(
            patch('confluent_kafka_helpers.loader.AvroSchemaRegistry', schema_registry)
        )
        yield schema_registry
