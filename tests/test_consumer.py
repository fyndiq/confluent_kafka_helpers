from unittest.mock import Mock, call, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException

from confluent_kafka_helpers.consumer import default_error_handler, get_message
from confluent_kafka_helpers.exceptions import EndOfPartition, KafkaTransportError

from tests.kafka import KafkaError, KafkaMessage


class TestAvroConsumer:
    def test_init(self, avro_consumer, confluent_avro_consumer):
        assert avro_consumer.topics == ['a']
        confluent_avro_consumer.subscribe.assert_called_once_with(['a'])
        confluent_avro_consumer.assert_called_once()

    def test_consume_messages(self, avro_consumer):
        with pytest.raises(RuntimeError):
            for message in avro_consumer:
                assert message.value == b'foobar'

    @patch('confluent_kafka_helpers.consumer.tracer')
    def test_consume_messages_adds_tracing(self, tracer, avro_consumer):
        with pytest.raises(RuntimeError):
            for message in avro_consumer:
                expected_calls = [
                    call.extract_headers_and_start_span(
                        operation_name='kafka.consumer.consume', headers={'foo': 'bar'}
                    ),
                    call.extract_headers_and_start_span().__enter__(),
                    call.extract_headers_and_start_span()
                    .__enter__()
                    .set_tag('span.kind', 'consumer'),
                    call.extract_headers_and_start_span()
                    .__enter__()
                    .set_tag('message_bus.destination', 'test'),
                    call.extract_headers_and_start_span().__enter__().set_tag('message_bus.key', 1),
                    call.extract_headers_and_start_span()
                    .__enter__()
                    .set_tag('message_bus.offset', 0),
                    call.extract_headers_and_start_span()
                    .__enter__()
                    .set_tag('message_bus.partition', 1),
                ]
                tracer.assert_has_calls(expected_calls)


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

    @pytest.mark.parametrize(
        'code',
        [
            (ConfluentKafkaError._ALL_BROKERS_DOWN),
            (ConfluentKafkaError._NO_OFFSET),
            (ConfluentKafkaError._TIMED_OUT),
        ],
    )
    def test_raises_kafkaexception_on_other_errors(self, code):
        error = KafkaError(_code=code)
        with pytest.raises(KafkaException):
            default_error_handler(error)
