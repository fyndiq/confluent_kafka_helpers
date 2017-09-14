import pytest

from unittest.mock import patch, MagicMock

from confluent_kafka_helpers import consumer
from confluent_kafka_helpers.test import config


class PollReturnMock:
    value = MagicMock()
    value.return_value = b'foobar'
    error = MagicMock()
    error.return_value = None


class ConfluentAvroConsumerMock:
    def __init__(self, config):
        pass

    subscribe = MagicMock()
    poll = MagicMock()
    poll.return_value = PollReturnMock()
    close = MagicMock()


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


def test_avro_consumer_init(avro_consumer):
    assert avro_consumer.topic == ['a']
    assert avro_consumer.config == config.Config.KAFKA_REPOSITORY_LOADER_CONFIG
    assert avro_consumer.timeout == 1.0
    mock_confluent_avro_consumer.subscribe.assert_called_once_with(
        ['a']
    )


def test_exit(avro_consumer):
    avro_consumer.__exit__(1, 1, 1)
    assert mock_confluent_avro_consumer.close.call_count == 1


@patch('confluent_kafka.KafkaException', MagicMock())
def test_avro_consumer(avro_consumer):
    for message in avro_consumer:
        assert message.value() == b'foobar'
        break
