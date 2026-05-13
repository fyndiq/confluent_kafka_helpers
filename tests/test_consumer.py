from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException
from opentelemetry.trace import SpanKind

from confluent_kafka_helpers.consumer import (
    default_error_handler,
    get_message,
    is_kafka_transient_error,
)
import confluent_kafka_helpers
from confluent_kafka_helpers.exceptions import EndOfPartition, KafkaTransportError

from tests.kafka import KafkaError, KafkaMessage


class TestAvroConsumer:
    def test_init(self, avro_consumer):
        consumer = avro_consumer()
        assert consumer.topics == ["a"]
        consumer._mock_consumer.assert_called_once()
        consumer.consumer.subscribe.assert_called_once_with(["a"])

    def test_consume_messages(self, avro_consumer):
        with pytest.raises(RuntimeError):
            with avro_consumer() as consumer:
                for message in consumer:
                    assert message.value == b"foobar"

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_consume_messages_adds_tracing(self, tracer, avro_consumer):
        with pytest.raises(RuntimeError):
            with avro_consumer() as consumer:
                for message in consumer:
                    pass

        expected_calls = [
            call.extract_headers(headers={"foo": "bar"}),
            call.extract_links(context=ANY),
            call.start_span(
                name="kafka.consume",
                kind=SpanKind.CONSUMER,
                resource_name="test",
                context=ANY,
                links=ANY,
            ),
            call.start_span().__enter__(),
            call.start_span(name="kafka.create_message"),
            call.start_span().__enter__(),
            call.start_span().__exit__(None, None, None),
            call.start_span().__enter__().set_attribute("messaging.operation.name", "consume"),
            call.start_span().__enter__().set_attribute("messaging.operation.type", "receive"),
            call.start_span().__enter__().set_attribute("messaging.client.id", "<client-id>"),
            call.start_span().__enter__().set_attribute("messaging.destination.name", "test"),
            call.start_span().__enter__().set_attribute("messaging.consumer.group.name", 1),
            call.start_span().__enter__().set_attribute("messaging.destination.partition.id", 1),
            call.start_span().__enter__().set_attribute("messaging.kafka.message.offset", 0),
            call.start_span().__enter__().set_attribute("messaging.kafka.message.key", 1),
            call.start_span().__enter__().set_attribute("server.address", "localhost"),
            call.start_span().__enter__().set_attribute("server.port", "9092"),
        ]
        tracer.assert_has_calls(expected_calls)

    def test_context_manager_close_consumer(self, mocker, avro_consumer):
        consumer = avro_consumer()
        mock_consumer = mocker.spy(consumer, "consumer")
        with consumer:
            pass
        mock_consumer.close.assert_called_once()
        mock_consumer.reset_mock()

        with pytest.raises(ZeroDivisionError):
            with consumer:
                1 / 0
        mock_consumer.close.assert_called_once()

    def test_context_manager_close_generator(self, mocker, avro_consumer):
        consumer = avro_consumer()
        mock_generator = mocker.spy(consumer, "_generator")
        with consumer:
            pass
        mock_generator.close.assert_called_once()
        mock_generator.reset_mock()

        with pytest.raises(ZeroDivisionError):
            with consumer:
                1 / 0
        mock_generator.close.assert_called_once()

    @patch("confluent_kafka_helpers.consumer.set_propagated_headers")
    def test_sets_propagated_headers(self, set_propagated_headers, avro_consumer):
        headers = [
            ("x-request-id", b"abc-123"),
            ("foo", b"bar"),
            ("x-correlation-id", b"xyz-789"),
        ]
        config_override = {"headers.propagate": ["x-request-id", "x-correlation-id"]}

        consumer = avro_consumer(config_override=config_override, headers=headers)

        next(iter(consumer))

        set_propagated_headers.assert_called_once_with(
            {"x-request-id": "abc-123", "x-correlation-id": "xyz-789"}
        )


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
        "code",
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


class TestIsKafkaTransientError:
    @pytest.mark.parametrize(
        "code",
        [
            ConfluentKafkaError.REQUEST_TIMED_OUT,
            ConfluentKafkaError.BROKER_NOT_AVAILABLE,
        ],
    )
    def test_returns_true_for_transient_codes(self, code):
        exc = KafkaException(ConfluentKafkaError(code))
        assert is_kafka_transient_error(exc) is True

    @pytest.mark.parametrize(
        "code",
        [
            ConfluentKafkaError._ALL_BROKERS_DOWN,
            ConfluentKafkaError._TIMED_OUT,
            ConfluentKafkaError._TRANSPORT,
            ConfluentKafkaError.OFFSET_OUT_OF_RANGE,
            ConfluentKafkaError.UNKNOWN_TOPIC_OR_PART,
        ],
    )
    def test_returns_false_for_non_transient_codes(self, code):
        exc = KafkaException(ConfluentKafkaError(code))
        assert is_kafka_transient_error(exc) is False

    def test_returns_false_for_exception_without_args(self):
        exc = KafkaException()
        assert is_kafka_transient_error(exc) is False


class TestAvroConsumerCommit:
    def test_successful_commit_does_not_retry(self, avro_consumer):
        consumer = avro_consumer()
        consumer.consumer.commit = Mock()

        consumer.commit()

        assert consumer.consumer.commit.call_count == 1

    @pytest.mark.parametrize(
        "code",
        [
            ConfluentKafkaError.REQUEST_TIMED_OUT,
            ConfluentKafkaError.BROKER_NOT_AVAILABLE,
        ],
    )
    def test_retries_on_transient_errors(self, code, avro_consumer):
        consumer = avro_consumer()
        transient_error = KafkaException(ConfluentKafkaError(code))
        consumer.consumer.commit = Mock(side_effect=[transient_error, transient_error, None])

        consumer.commit()

        assert consumer.consumer.commit.call_count == 3

    @pytest.mark.parametrize(
        "code",
        [
            ConfluentKafkaError.REQUEST_TIMED_OUT,
            ConfluentKafkaError.BROKER_NOT_AVAILABLE,
        ],
    )
    def test_reraises_after_max_retries_on_transient_error(self, code, avro_consumer):
        consumer = avro_consumer()
        transient_error = KafkaException(ConfluentKafkaError(code))
        consumer.consumer.commit = Mock(side_effect=transient_error)

        with pytest.raises(KafkaException) as exc_info:
            consumer.commit()

        assert exc_info.value.args[0].code() == code
        assert consumer.consumer.commit.call_count == 3

    @pytest.mark.parametrize(
        "code",
        [
            ConfluentKafkaError._ALL_BROKERS_DOWN,
            ConfluentKafkaError.OFFSET_OUT_OF_RANGE,
            ConfluentKafkaError.UNKNOWN_TOPIC_OR_PART,
        ],
    )
    def test_does_not_retry_on_non_transient_errors(self, code, avro_consumer):
        consumer = avro_consumer()
        non_transient = KafkaException(ConfluentKafkaError(code))
        consumer.consumer.commit = Mock(side_effect=non_transient)

        with pytest.raises(KafkaException):
            consumer.commit()

        assert consumer.consumer.commit.call_count == 1
@pytest.fixture(autouse=True)
def _reset_shutdown_state_for_consumer_tests():
    """Tests in TestGracefulShutdown set the module-level flag. Clear it
    before/after every test so unrelated tests don't observe a stale flag."""
    flag = getattr(confluent_kafka_helpers, "shutdown_requested", None)
    if flag is not None:
        flag.clear()
    yield
    flag = getattr(confluent_kafka_helpers, "shutdown_requested", None)
    if flag is not None:
        flag.clear()


class TestGracefulShutdown:
    @staticmethod
    def _patch_poll_to_be_inexhaustible(consumer, confluent_message):
        """Default fixtures use side_effect=[msg, StopIteration] which makes
        the second poll exhaust and produce a RuntimeError (PEP 479). For
        these tests we need poll to return messages forever so that the
        ONLY thing that stops iteration is the shutdown flag."""
        message = confluent_message()
        consumer.consumer.poll = MagicMock(side_effect=lambda timeout: message)
        consumer._generator = consumer._message_generator()

    def test_generator_stops_when_shutdown_requested_after_first_message(
        self, avro_consumer, confluent_message
    ):
        consumer = avro_consumer()
        self._patch_poll_to_be_inexhaustible(consumer, confluent_message)

        iterator = iter(consumer)
        first = next(iterator)
        assert first.value == b"foobar"

        confluent_kafka_helpers.shutdown_requested.set()

        with pytest.raises(StopIteration):
            next(iterator)

    def test_for_loop_exits_after_processing_in_flight_message(
        self, avro_consumer, confluent_message
    ):
        consumer = avro_consumer()
        self._patch_poll_to_be_inexhaustible(consumer, confluent_message)

        processed = []
        for i, message in enumerate(consumer):
            processed.append(message)
            confluent_kafka_helpers.shutdown_requested.set()
            if i >= 5:
                pytest.fail(
                    "Consumer kept yielding messages after shutdown_requested "
                    "was set — drain-on-shutdown is not implemented."
                )

        assert len(processed) == 1

    def test_consumer_close_runs_after_graceful_shutdown(
        self, avro_consumer, confluent_message, mocker
    ):
        consumer = avro_consumer()
        self._patch_poll_to_be_inexhaustible(consumer, confluent_message)

        close_spy = mocker.spy(consumer.consumer, "close")

        with consumer as c:
            for i, _ in enumerate(c):
                confluent_kafka_helpers.shutdown_requested.set()
                if i >= 5:
                    pytest.fail(
                        "Consumer kept yielding messages after shutdown_requested "
                        "was set — drain-on-shutdown is not implemented."
                    )

        close_spy.assert_called_once()

    def test_no_messages_yielded_if_shutdown_requested_before_iteration(
        self, avro_consumer, confluent_message
    ):
        consumer = avro_consumer()
        self._patch_poll_to_be_inexhaustible(consumer, confluent_message)

        confluent_kafka_helpers.shutdown_requested.set()

        with pytest.raises(StopIteration):
            next(iter(consumer))
