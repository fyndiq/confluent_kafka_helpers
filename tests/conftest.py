from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer

from confluent_kafka_helpers import consumer

from tests import config

number_of_messages = 1


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


class ConfluentAvroConsumerMock(MagicMock):
    subscribe = MagicMock()
    poll = MagicMock(name='poll', side_effect=[PollReturnMock(), StopIteration])
    close = MagicMock()
    get_watermark_offsets = MagicMock(
        name='watermark_test', return_value=[1, number_of_messages]
    )


mock_confluent_avro_consumer = ConfluentAvroConsumerMock(
    spec=ConfluentAvroConsumer, name='ConfluentAvroConsumerMock'
)


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.consumer.ConfluentAvroConsumer',
    mock_confluent_avro_consumer
)
def avro_consumer():
    consumer_config = config.Config.KAFKA_CONSUMER_CONFIG
    return consumer.AvroConsumer(consumer_config)
