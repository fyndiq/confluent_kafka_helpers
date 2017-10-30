from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer
from confluent_kafka_helpers import consumer

from test import config

number_of_messages = 1


class PollReturnMock:
    value = MagicMock()
    value.return_value = b'foobar'
    error = MagicMock()
    error.return_value = None
    key = MagicMock()
    key.return_value = 1
    offset = MagicMock()
    offset.side_effect = [i for i in range(0, number_of_messages)]


class ConfluentAvroConsumerMock(MagicMock):
    subscribe = MagicMock()
    poll = MagicMock(name='poll', return_value=PollReturnMock())
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
