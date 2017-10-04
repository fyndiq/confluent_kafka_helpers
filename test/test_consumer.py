from unittest.mock import MagicMock, patch

from test import config
from test import conftest


mock_confluent_avro_consumer = conftest.mock_confluent_avro_consumer


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
