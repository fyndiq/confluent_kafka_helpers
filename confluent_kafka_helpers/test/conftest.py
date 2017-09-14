from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka_helpers.test import config
from confluent_kafka_helpers import consumer


class PollReturnMock:
    value = MagicMock()
    value.return_value = b'foobar'
    error = MagicMock()
    error.return_value = None
    key = MagicMock()
    key.return_value = 1
    offset = MagicMock()
    offset.return_value = 1


class ConfluentAvroConsumerMock(MagicMock):
    subscribe = MagicMock()
    poll = MagicMock(name='poll')
    poll.return_value = PollReturnMock()
    close = MagicMock()
    #get_watermark_offsets = MagicMock(name='watermark')
    #get_watermark_offsets.return_value = (1, 2)


mock_confluent_avro_consumer = ConfluentAvroConsumerMock


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.consumer.ConfluentAvroConsumer',
    mock_confluent_avro_consumer
)
def avro_consumer():
    consumer_config = config.Config.KAFKA_REPOSITORY_LOADER_CONFIG
    topic = 'a'
    timeout = 1.0
    return consumer.AvroConsumer(topic, consumer_config, timeout)
