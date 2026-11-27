import inspect
from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.avro import loads
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

import confluent_kafka_helpers
from confluent_kafka_helpers.consumer import (
    AvroBatchConsumer,
    default_error_handler,
    get_message,
    is_kafka_transient_error,
)
from confluent_kafka_helpers.context import get_propagated_headers, propagated_headers
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

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_consume_messages_adds_propagated_headers_to_span(self, tracer, avro_consumer):
        headers = [
            ("x-request-id", b"abc-123"),
            ("correlation_id", b"xyz-789"),
            ("foo", b"bar"),
        ]
        consumer = avro_consumer(
            config_override={"headers.propagate": ["x-request-id", "correlation_id"]},
            headers=headers,
        )

        next(iter(consumer))

        tracer.start_span().__enter__().set_attribute.assert_any_call(
            "confluent_kafka_helpers.header.x-request-id", "abc-123"
        )
        tracer.start_span().__enter__().set_attribute.assert_any_call(
            "messaging.message.conversation_id", "xyz-789"
        )

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

    @patch("confluent_kafka_helpers.consumer.set_propagated_headers")
    def test_sets_default_propagated_headers_without_config(
        self, set_propagated_headers, avro_consumer
    ):
        headers = [
            ("origin_action_type", b"batch_export"),
            ("action_started_at", b"1729170000000"),
            ("foo", b"bar"),
        ]

        consumer = avro_consumer(headers=headers)

        next(iter(consumer))

        assert consumer.propagate_header_keys == {"origin_action_type", "action_started_at"}
        set_propagated_headers.assert_called_once_with(
            {
                "origin_action_type": "batch_export",
                "action_started_at": "1729170000000",
            }
        )

    @patch("confluent_kafka_helpers.consumer.set_propagated_headers")
    def test_configured_propagated_headers_are_appended_to_defaults_and_deduped(
        self, set_propagated_headers, avro_consumer
    ):
        headers = [
            ("origin_action_type", b"batch_export"),
            ("action_started_at", b"1729170000000"),
            ("correlation_id", b"abc-123"),
        ]
        config_override = {
            "headers.propagate": [
                "origin_action_type",
                "correlation_id",
                "action_started_at",
                "correlation_id",
            ]
        }

        consumer = avro_consumer(config_override=config_override, headers=headers)

        next(iter(consumer))

        assert consumer.propagate_header_keys == {
            "origin_action_type",
            "action_started_at",
            "correlation_id",
        }
        set_propagated_headers.assert_called_once_with(
            {
                "origin_action_type": "batch_export",
                "action_started_at": "1729170000000",
                "correlation_id": "abc-123",
            }
        )

    @patch("confluent_kafka_helpers.consumer.set_propagated_headers")
    def test_default_propagated_headers_are_not_invented_when_absent(
        self, set_propagated_headers, avro_consumer
    ):
        headers = [("foo", b"bar")]

        consumer = avro_consumer(headers=headers)

        next(iter(consumer))

        set_propagated_headers.assert_called_once_with({})

    def test_batch_config_defaults(self, avro_consumer):
        consumer = avro_consumer(consumer_class=AvroBatchConsumer)
        assert consumer.batch_size == 100
        assert consumer.timeout == 0.1

    def test_batch_config_is_popped_from_kafka_config(self, avro_consumer):
        consumer = avro_consumer(consumer_class=AvroBatchConsumer, batch_size=5, timeout=2.5)

        assert consumer.batch_size == 5
        assert consumer.timeout == 2.5
        kafka_config = consumer._mock_consumer.call_args.args[0]
        assert "batch_max_size" not in kafka_config
        assert "batch_max_wait" not in kafka_config


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
            ConfluentKafkaError.COORDINATOR_LOAD_IN_PROGRESS,
            ConfluentKafkaError.NOT_COORDINATOR,
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
            ConfluentKafkaError.COORDINATOR_LOAD_IN_PROGRESS,
            ConfluentKafkaError.NOT_COORDINATOR,
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
            ConfluentKafkaError.COORDINATOR_LOAD_IN_PROGRESS,
            ConfluentKafkaError.NOT_COORDINATOR,
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
        """Forthese tests we need poll to return messages forever so that the
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

        confluent_kafka_helpers.set_shutdown_requested()

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
            confluent_kafka_helpers.set_shutdown_requested()
            if i >= 5:
                pytest.fail(
                    "Consumer kept yielding messages after shutdown_requested "
                    "was set — stop-after-current-message is not implemented."
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
                confluent_kafka_helpers.set_shutdown_requested()
                if i >= 5:
                    pytest.fail(
                        "Consumer kept yielding messages after shutdown_requested "
                        "was set — stop-after-current-message is not implemented."
                    )

        close_spy.assert_called_once()

    def test_no_messages_yielded_if_shutdown_requested_before_iteration(
        self, avro_consumer, confluent_message
    ):
        consumer = avro_consumer()
        self._patch_poll_to_be_inexhaustible(consumer, confluent_message)

        confluent_kafka_helpers.set_shutdown_requested()

        with pytest.raises(StopIteration):
            next(iter(consumer))


@pytest.fixture
def batch_consumer(avro_consumer):
    def _create(items, config_override=None, **kwargs):
        config_override = dict(config_override or {})
        consumer = avro_consumer(
            config_override=config_override, consumer_class=AvroBatchConsumer, **kwargs
        )
        pending = list(items)

        def read(num_messages, timeout):
            result = []
            while pending and len(result) < num_messages:
                item = pending.pop(0)
                if item is None:
                    break
                result.append(item)
            if not pending and not result:
                confluent_kafka_helpers.set_shutdown_requested()
            return result

        consumer.consumer.consume = Mock(side_effect=read)
        consumer.consumer._serializer = Mock()
        consumer.consumer._serializer.decode_message.side_effect = lambda data, is_key: data
        consumer.consumer.assignment.return_value = [TopicPartition("test", 1)]
        consumer.consumer.commit.return_value = []
        return consumer

    return _create


class TestAvroConsumerBatches:
    def test_explicit_error_handler_can_skip_record(self, batch_consumer, confluent_message):
        bad = confluent_message()
        error = ConfluentKafkaError(ConfluentKafkaError._TRANSPORT)
        bad.error.return_value = error
        handler = Mock()
        consumer = batch_consumer([bad, confluent_message()], error_handler=handler)
        assert [len(batch) for batch in consumer] == [1]
        handler.assert_called_once_with(error)

    def test_rejects_single_message_hook(self, batch_consumer):
        with pytest.raises(TypeError, match="get_message"):
            batch_consumer([], get_message=Mock())

    @pytest.mark.parametrize("size", [0, -1, True, 1.0, 1_000_001])
    def test_invalid_size(self, batch_consumer, size):
        with pytest.raises(ValueError, match="batch_size"):
            batch_consumer([], batch_size=size)

    @pytest.mark.parametrize("timeout", [True, -1, float("inf"), float("nan"), "1"])
    def test_invalid_timeout(self, batch_consumer, timeout):
        with pytest.raises(ValueError, match="timeout"):
            batch_consumer([], timeout=timeout)

    @pytest.mark.parametrize("option", ["poll_timeout", "non_blocking", "batch_max_size"])
    def test_rejects_polling_options(self, batch_consumer, option):
        with pytest.raises(ValueError, match=option):
            batch_consumer([], {option: 1})

    def test_rejects_automatic_offset_storage(self, batch_consumer):
        with pytest.raises(ValueError, match="enable.auto.offset.store"):
            batch_consumer([], {"enable.auto.offset.store": True})

    def test_commits_explicit_offsets_per_partition(self, batch_consumer, confluent_message):
        records = [confluent_message() for _ in range(3)]
        for record, partition, offset in zip(records, [0, 1, 0], [4, 9, 6]):
            record.partition.return_value = partition
            record.offset.return_value = offset
        consumer = batch_consumer(records)
        batch = next(consumer)
        consumer.commit_batch(batch)
        offsets = consumer.consumer.commit.call_args.kwargs
        assert offsets["asynchronous"] is False
        assert [(p.topic, p.partition, p.offset) for p in offsets["offsets"]] == [
            ("test", 0, 7),
            ("test", 1, 10),
        ]
        assert consumer.config["enable.auto.commit"] is False
        assert consumer.config["enable.auto.offset.store"] is False
        consumer.commit_batch([])
        assert consumer.consumer.commit.call_count == 1

    @pytest.mark.parametrize("partition_error", [True, False])
    def test_commit_propagates_assignment_loss(
        self, batch_consumer, confluent_message, partition_error
    ):
        consumer = batch_consumer([confluent_message()])
        batch = next(consumer)
        error = ConfluentKafkaError(ConfluentKafkaError._ASSIGNMENT_LOST)
        if partition_error:
            consumer.consumer.commit.return_value = [Mock(error=error)]
        else:
            consumer.consumer.commit.side_effect = KafkaException(error)
        with pytest.raises(KafkaException):
            consumer.commit_batch(batch)
        consumer.consumer.commit.assert_called_once()

    def test_waits_for_eof_in_all_partitions(self, batch_consumer, confluent_message):
        def eof(partition):
            record = confluent_message()
            record.partition.return_value = partition
            record.error.return_value = ConfluentKafkaError(ConfluentKafkaError._PARTITION_EOF)
            return record

        message = confluent_message()
        consumer = batch_consumer([], {"stop_on_eof": True})
        consumer.consumer.assignment.return_value = [
            TopicPartition("test", 0),
            TopicPartition("test", 1),
        ]
        consumer.consumer.consume.side_effect = [[eof(0)], [message], [eof(1)]]
        assert [len(batch) for batch in consumer] == [1]
        assert consumer.consumer.consume.call_count == 3
        callbacks = consumer.consumer.subscribe.call_args.kwargs
        callbacks["on_lost"](consumer.consumer, [TopicPartition("test", 0)])
        assert ("test", 0) not in consumer._eof_partitions

    def test_restores_ambient_headers_on_close(self, batch_consumer, confluent_message):
        with propagated_headers({"outside": "scope"}):
            with batch_consumer([confluent_message()]) as consumer:
                next(consumer)
                assert get_propagated_headers() == {}
            assert get_propagated_headers() == {"outside": "scope"}

    def test_decode_failure_terminates_fetch(self, batch_consumer, confluent_message):
        consumer = batch_consumer([confluent_message(), confluent_message()])
        consumer.consumer._serializer.decode_message.side_effect = SerializerError("bad avro")
        with pytest.raises(SerializerError, match="offset 0"):
            next(consumer)
        assert list(consumer) == []
        consumer.consumer.consume.assert_called_once()

    def test_rejects_auto_commit_before_reading(self, batch_consumer, confluent_message):
        with pytest.raises(ValueError, match="enable.auto.commit"):
            batch_consumer([confluent_message()], config_override={"enable.auto.commit": True})

    def test_auto_commit_still_allows_per_message_iteration(self, avro_consumer):
        consumer = avro_consumer(config_override={"enable.auto.commit": True})

        assert next(consumer).value == b"foobar"

    @pytest.mark.parametrize("batch_size", [10, 100])
    def test_yields_full_batches_then_partial_before_shutdown(
        self, batch_consumer, confluent_message, batch_size
    ):
        message = confluent_message()
        partial_size = batch_size // 2
        consumer = batch_consumer(
            [message] * (2 * batch_size + partial_size),
            batch_size=batch_size,
        )

        batches = list(consumer)

        assert [len(b) for b in batches] == [batch_size, batch_size, partial_size]
        assert all(m.value == b"foobar" for b in batches for m in b)

    def test_blocking_idle_consumer_yields_nothing(self, batch_consumer):
        consumer = batch_consumer([None, None, None])

        assert list(consumer) == []

    def test_no_batches_if_shutdown_requested_before_iteration(
        self, batch_consumer, confluent_message
    ):
        consumer = batch_consumer([confluent_message()])
        confluent_kafka_helpers.set_shutdown_requested()

        assert list(consumer) == []
        consumer.consumer.consume.assert_not_called()

    def test_does_not_yield_partial_batch_when_shutdown_requested_during_read(
        self, batch_consumer, confluent_message
    ):
        message = confluent_message()
        consumer = batch_consumer([message, message], batch_size=10, timeout=1.0)

        def consume_then_shutdown(num_messages, timeout):
            confluent_kafka_helpers.set_shutdown_requested()
            return [message, message]

        consumer.consumer.consume = Mock(side_effect=consume_then_shutdown)

        assert list(consumer) == []
        consumer.consumer.consume.assert_called_once_with(num_messages=10, timeout=1.0)

    def test_context_manager_closes_batch_generator(self, batch_consumer, confluent_message):
        consumer = batch_consumer([confluent_message()], batch_size=1)

        with consumer:
            batches = consumer._generator
            next(batches)

        assert inspect.getgeneratorstate(batches) == inspect.GEN_CLOSED

    def test_native_read_decodes_avro_key_and_value(self, batch_consumer, confluent_message):
        message = confluent_message()
        registry = Mock()
        registry.get_by_id.return_value = loads('"string"')
        serde = MessageSerializer(registry)
        message.set_key(serde.encode_record_with_schema_id(1, "key", is_key=True))
        message.set_value(serde.encode_record_with_schema_id(1, "value"))
        wire_key, wire_value = message.key(), message.value()
        tombstone = confluent_message()
        tombstone.set_key(None)
        tombstone.set_value(None)
        consumer = batch_consumer([message, tombstone], batch_size=10, timeout=2)
        consumer.consumer._serializer = serde

        (batch,) = list(consumer)

        assert [(m._meta.key, m.value) for m in batch] == [("key", "value"), (None, None)]
        assert batch[0]._raw is message
        assert (message.key(), message.value()) == (wire_key, wire_value)
        assert consumer.consumer.consume.call_args_list[0] == call(num_messages=10, timeout=2)

    def test_skips_empty_reads(self, batch_consumer, confluent_message):
        consumer = batch_consumer([None, None, confluent_message()])
        assert [len(b) for b in consumer] == [1]

    def test_yields_partial_read(self, batch_consumer, confluent_message):
        message = confluent_message()
        consumer = batch_consumer([message, None, message], timeout=0.1)

        assert [len(b) for b in consumer] == [1, 1]
        consumer.consumer.consume.assert_called_with(num_messages=100, timeout=0.1)

    def test_stop_on_eof_yields_partial_batch_then_stops(self, batch_consumer, confluent_message):
        eof = confluent_message()
        eof.error.return_value = KafkaError(_code=ConfluentKafkaError._PARTITION_EOF)
        message = confluent_message()
        consumer = batch_consumer([message, message, eof, None, message], {"stop_on_eof": True})

        batches = list(consumer)

        assert [len(b) for b in batches] == [2]

    def test_error_propagates_and_buffered_messages_are_not_yielded(
        self, batch_consumer, confluent_message
    ):
        error = confluent_message()
        error.error.return_value = KafkaError(_code=ConfluentKafkaError._ALL_BROKERS_DOWN)
        consumer = batch_consumer([confluent_message(), error])

        yielded = []
        with pytest.raises(KafkaException):
            for batch in consumer:
                yielded.append(batch)

        assert yielded == []

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_one_span_per_batch_linked_to_each_message(
        self, tracer, batch_consumer, confluent_message
    ):
        tracer.extract_links.return_value = ["<link>"]
        message = confluent_message()
        consumer = batch_consumer([message] * 3)

        list(consumer)

        tracer.start_span.assert_called_once_with(
            name="kafka.consume",
            kind=SpanKind.CONSUMER,
            resource_name="test",
            context=ANY,
            links=["<link>", "<link>", "<link>"],
        )
        assert tracer.start_span.call_args.kwargs["context"] == Context()  # new root trace
        span = tracer.start_span.return_value.__enter__.return_value
        span.set_attribute.assert_any_call("messaging.batch.message_count", 3)
        span.set_attribute.assert_any_call("messaging.destination.name", "test")
        span.set_attribute.assert_any_call("messaging.consumer.group.name", 1)
        span.set_attribute.assert_any_call("server.address", "localhost")

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_batch_span_uses_default_link_budget(
        self, tracer, batch_consumer, confluent_message, monkeypatch
    ):
        monkeypatch.delenv("OTEL_SPAN_LINK_COUNT_LIMIT", raising=False)
        tracer.extract_links.return_value = ["<link>"]
        consumer = batch_consumer([confluent_message()] * 130, batch_size=130)

        list(consumer)

        assert tracer.extract_headers.call_count == 128
        assert tracer.extract_links.call_count == 128
        assert tracer.start_span.call_args.kwargs["links"] == ["<link>"] * 128

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_batch_span_uses_env_link_budget(
        self, tracer, batch_consumer, confluent_message, monkeypatch
    ):
        monkeypatch.setenv("OTEL_SPAN_LINK_COUNT_LIMIT", "2")
        tracer.extract_headers.side_effect = lambda *, headers: headers["traceparent"]
        tracer.extract_links.side_effect = lambda *, context: [f"<link:{context}>"]
        messages = [confluent_message(headers=[("traceparent", f"trace-{i}")]) for i in range(4)]
        consumer = batch_consumer(messages)

        list(consumer)

        assert tracer.extract_headers.call_count == 2
        assert tracer.extract_links.call_count == 2
        assert tracer.start_span.call_args.kwargs["links"] == ["<link:trace-0>", "<link:trace-1>"]

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_batch_span_zero_link_budget_skips_extraction(
        self, tracer, batch_consumer, confluent_message, monkeypatch
    ):
        monkeypatch.setenv("OTEL_SPAN_LINK_COUNT_LIMIT", "0")
        messages = [confluent_message(), confluent_message()]
        consumer = batch_consumer(messages)

        batches = list(consumer)

        assert [len(batch) for batch in batches] == [2]
        tracer.extract_headers.assert_not_called()
        tracer.extract_links.assert_not_called()
        assert tracer.start_span.call_args.kwargs["links"] == []

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_batch_span_missing_trace_contexts_do_not_use_link_budget(
        self, tracer, batch_consumer, confluent_message, monkeypatch
    ):
        monkeypatch.setenv("OTEL_SPAN_LINK_COUNT_LIMIT", "2")
        tracer.extract_headers.side_effect = lambda *, headers: headers.get("traceparent")
        tracer.extract_links.side_effect = lambda *, context: (
            [] if context is None else [f"<link:{context}>"]
        )
        messages = [
            confluent_message(headers=[]),
            confluent_message(headers=[("traceparent", "first")]),
            confluent_message(headers=[]),
            confluent_message(headers=[("traceparent", "second")]),
            confluent_message(headers=[("traceparent", "third")]),
        ]
        consumer = batch_consumer(messages)

        list(consumer)

        assert tracer.extract_headers.call_count == 4
        assert tracer.extract_links.call_count == 4
        assert tracer.start_span.call_args.kwargs["links"] == ["<link:first>", "<link:second>"]

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_batch_span_invalid_links_do_not_use_link_budget(
        self, tracer, batch_consumer, confluent_message, monkeypatch
    ):
        monkeypatch.setenv("OTEL_SPAN_LINK_COUNT_LIMIT", "1")
        invalid_link = Mock()
        invalid_link.context.is_valid = False
        valid_link = Mock()
        valid_link.context.is_valid = True
        tracer.extract_links.side_effect = [[invalid_link], [valid_link], [Mock()]]
        consumer = batch_consumer([confluent_message()] * 3)

        list(consumer)

        assert tracer.extract_headers.call_count == 2
        assert tracer.extract_links.call_count == 2
        assert tracer.start_span.call_args.kwargs["links"] == [valid_link]

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_mixed_topic_batch_has_no_destination_name(
        self, tracer, batch_consumer, confluent_message
    ):
        other = confluent_message()
        other.topic.return_value = "other"
        consumer = batch_consumer([confluent_message(), other])

        list(consumer)

        assert tracer.start_span.call_args.kwargs["resource_name"] == "other,test"
        span = tracer.start_span.return_value.__enter__.return_value
        assert call("messaging.destination.name", ANY) not in span.set_attribute.call_args_list

    @patch("confluent_kafka_helpers.consumer.set_propagated_headers")
    def test_does_not_set_propagated_headers(
        self, set_propagated_headers, batch_consumer, confluent_message
    ):
        consumer = batch_consumer(
            [confluent_message(headers=[("x-request-id", b"abc")])],
            config_override={"headers.propagate": ["x-request-id"]},
        )

        list(consumer)

        set_propagated_headers.assert_not_called()

    @patch("confluent_kafka_helpers.consumer.statsd")
    def test_increments_message_count_by_batch_length(
        self, statsd, batch_consumer, confluent_message
    ):
        consumer = batch_consumer([confluent_message()] * 3)

        list(consumer)

        statsd.increment.assert_called_once_with(
            "confluent_kafka_helpers.consumer.message.count.total", 3
        )


class TestPropagatedHeadersWhileHandling:
    """`producer.produce()` reads `get_propagated_headers()`, so whatever it returns while a
    message is handled is what gets forwarded on produced messages."""

    @pytest.fixture
    def messages(self, confluent_message):
        return [
            confluent_message(headers=[("correlation_id", b"first"), ("foo", b"bar")]),
            confluent_message(headers=[("correlation_id", b"second")]),
        ]

    def test_single_consume_sets_each_messages_headers(self, avro_consumer, messages):
        consumer = avro_consumer(config_override={"headers.propagate": ["correlation_id"]})

        def poll(timeout):
            if messages:
                return messages.pop(0)
            confluent_kafka_helpers.set_shutdown_requested()

        consumer.consumer.poll = Mock(side_effect=poll)
        seen = [get_propagated_headers() for _ in consumer]

        assert seen == [{"correlation_id": "first"}, {"correlation_id": "second"}]

    def test_batch_consume_propagates_each_messages_headers(self, batch_consumer, messages):
        consumer = batch_consumer(
            messages, config_override={"headers.propagate": ["correlation_id"]}
        )
        (batch,) = list(consumer)  # e.g. after the bulk db write is committed

        seen = []
        for message in batch:
            with message.propagate():
                seen.append(get_propagated_headers())

        assert seen == [{"correlation_id": "first"}, {"correlation_id": "second"}]
        assert [m.propagated_headers for m in batch] == seen
        assert get_propagated_headers() == {}

    def test_propagate_restores_outer_headers(self, batch_consumer, messages):
        consumer = batch_consumer(
            messages, config_override={"headers.propagate": ["correlation_id"]}
        )
        (first, second) = next(consumer)

        with first.propagate():
            with second.propagate():
                assert get_propagated_headers() == {"correlation_id": "second"}
            assert get_propagated_headers() == {"correlation_id": "first"}
