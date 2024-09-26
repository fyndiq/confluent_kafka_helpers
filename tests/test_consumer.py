from unittest.mock import ANY, Mock, call, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException
from opentelemetry.trace import SpanKind

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
            with avro_consumer as consumer:
                for message in consumer:
                    assert message.value == b'foobar'

    @patch('confluent_kafka_helpers.consumer.tracer')
    def test_consume_messages_adds_tracing(self, tracer, avro_consumer):
        with pytest.raises(RuntimeError):
            with avro_consumer as consumer:
                for message in consumer:
                    pass

        expected_calls = [
            call.extract_headers(headers={'foo': 'bar'}),
            call.extract_links(context=ANY),
            call.start_span(
                name='kafka.consume',
                kind=SpanKind.CONSUMER,
                resource_name='test',
                context=ANY,
                links=ANY,
            ),
            call.start_span().__enter__(),
            call.start_span(name='kafka.create_message'),
            call.start_span().__enter__(),
            call.start_span().__exit__(None, None, None),
            call.start_span().__enter__().set_attribute('messaging.operation.name', 'consume'),
            call.start_span().__enter__().set_attribute('messaging.operation.type', 'receive'),
            call.start_span().__enter__().set_attribute('messaging.client.id', '<client-id>'),
            call.start_span().__enter__().set_attribute('messaging.destination.name', 'test'),
            call.start_span().__enter__().set_attribute('messaging.consumer.group.name', 1),
            call.start_span().__enter__().set_attribute('messaging.destination.partition.id', 1),
            call.start_span().__enter__().set_attribute('messaging.kafka.message.offset', 0),
            call.start_span().__enter__().set_attribute('messaging.kafka.message.key', 1),
            call.start_span().__enter__().set_attribute('server.address', 'localhost'),
            call.start_span().__enter__().set_attribute('server.port', '9092'),
        ]
        tracer.assert_has_calls(expected_calls)

    def test_context_manager_close_consumer(self, mocker, avro_consumer):
        mock_consumer = mocker.spy(avro_consumer, "consumer")
        with avro_consumer:
            pass
        mock_consumer.close.assert_called_once()
        mock_consumer.reset_mock()

        with pytest.raises(ZeroDivisionError):
            with avro_consumer:
                1 / 0
        mock_consumer.close.assert_called_once()

    def test_context_manager_close_generator(self, mocker, avro_consumer):
        mock_generator = mocker.spy(avro_consumer, "_generator")
        with avro_consumer:
            pass
        mock_generator.close.assert_called_once()
        mock_generator.reset_mock()

        with pytest.raises(ZeroDivisionError):
            with avro_consumer:
                1 / 0
        mock_generator.close.assert_called_once()


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
