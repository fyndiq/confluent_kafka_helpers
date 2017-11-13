from test import conftest
from unittest.mock import MagicMock, patch

import pytest

mock_confluent_avro_consumer = conftest.mock_confluent_avro_consumer


def test_avro_consumer_init(avro_consumer):
    assert avro_consumer.topics == ['a']
    assert avro_consumer.poll_timeout == 0.1
    mock_confluent_avro_consumer.subscribe.assert_called_once_with(['a'])
    mock_confluent_avro_consumer.assert_called_once()


def test_exit(avro_consumer):
    with pytest.raises(SystemExit):
        avro_consumer.__exit__(1, 1, 1)
        assert mock_confluent_avro_consumer.close.call_count == 1


@patch('confluent_kafka.KafkaException', MagicMock())
def test_avro_consumer(avro_consumer):
    for message in avro_consumer:
        assert message.value() == b'foobar'
        break
