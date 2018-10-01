from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException

from confluent_kafka_helpers.consumer import default_error_handler, get_message
from confluent_kafka_helpers.exceptions import (
    EndOfPartition, KafkaTransportError
)

from tests import conftest
from tests.kafka import KafkaError, KafkaMessage

mock_confluent_avro_consumer = conftest.mock_confluent_avro_consumer


def test_avro_consumer_init(avro_consumer):
    assert avro_consumer.topics == ['a']
    mock_confluent_avro_consumer.subscribe.assert_called_once_with(['a'])
    mock_confluent_avro_consumer.assert_called_once()


@patch('confluent_kafka.KafkaException', MagicMock())
def test_avro_consumer(avro_consumer):
    for message in avro_consumer:
        assert message.value == b'foobar'
        break


class TestGetMessage:
    def setup_method(self, *args):
        self.message = KafkaMessage
        self.consumer = Mock()
        self.consumer.poll.return_value = self.message(_error=True)

    def test_retries_on_kafkatransporterror(self):
        error_handler = Mock(side_effect=KafkaTransportError)
        with pytest.raises(KafkaTransportError):
            get_message(self.consumer, error_handler)
        assert self.consumer.poll.call_count == 3

    def test_raises_endofpartition_when_stop_on_eof_is_true(self):
        error_handler = Mock(side_effect=EndOfPartition)
        with pytest.raises(EndOfPartition):
            get_message(self.consumer, error_handler, stop_on_eof=True)

    def test_returns_none_when_stop_on_eof_is_false(self):
        error_handler = Mock(side_effect=EndOfPartition)
        message = get_message(self.consumer, error_handler)
        assert message is None


class TestErrorHandler:
    def test_raises_endofpartition_on_kafkaerror_partition_eof(self):
        error = KafkaError(_code=ConfluentKafkaError._PARTITION_EOF)
        with pytest.raises(EndOfPartition):
            default_error_handler(error)

    @pytest.mark.parametrize('code', [
        (ConfluentKafkaError._ALL_BROKERS_DOWN),
        (ConfluentKafkaError._NO_OFFSET),
        (ConfluentKafkaError._TIMED_OUT)
    ])
    def test_raises_kafkaexception_on_other_errors(self, code):
        error = KafkaError(_code=code)
        with pytest.raises(KafkaException):
            default_error_handler(error)
