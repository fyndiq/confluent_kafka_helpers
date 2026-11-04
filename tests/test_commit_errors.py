from unittest.mock import Mock, call, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from confluent_kafka_helpers.callbacks import default_on_commit_cb
from confluent_kafka_helpers.metrics import base_metric, statsd


@pytest.mark.parametrize("code", [KafkaError.UNKNOWN_MEMBER_ID, KafkaError._ASSIGNMENT_LOST])
def test_lost_assignment_does_not_retry_and_reports_failure(avro_consumer, code):
    consumer = avro_consumer()
    error = KafkaError(code)
    consumer.consumer.commit.side_effect = KafkaException(error)
    with patch.object(statsd, "increment") as increment, patch(
        "confluent_kafka_helpers.consumer.logger"
    ) as logger, patch("confluent_kafka_helpers.utils.time.sleep") as sleep:
        assert consumer.commit(asynchronous=False) is None
    consumer.consumer.commit.assert_called_once_with(asynchronous=False)
    sleep.assert_not_called()
    increment.assert_called_once_with(
        f"{base_metric}.consumer.commit.count.error", tags=[f"code:{code}"]
    )
    logger.warning.assert_called_once_with(
        "Kafka assignment lost; offsets were not committed", error=str(error)
    )


@pytest.mark.parametrize("code", [KafkaError.UNKNOWN_MEMBER_ID, KafkaError.NOT_COORDINATOR])
def test_fatal_commit_error_is_never_suppressed_or_retried(avro_consumer, code):
    consumer = avro_consumer()
    consumer.consumer.commit.side_effect = KafkaException(KafkaError(code, fatal=True))
    with pytest.raises(KafkaException):
        consumer.commit(asynchronous=False)
    consumer.consumer.commit.assert_called_once_with(asynchronous=False)


def test_retry_backoff_and_arguments_are_preserved(avro_consumer):
    consumer = avro_consumer()
    consumer.consumer.commit.side_effect = KafkaException(KafkaError(KafkaError.NOT_COORDINATOR))
    message = Mock()
    with patch("confluent_kafka_helpers.utils.time.sleep") as sleep:
        with pytest.raises(KafkaException):
            consumer.commit(message, asynchronous=False)
    assert (
        consumer.consumer.commit.call_args_list
        == [
            call(message, asynchronous=False),
        ]
        * 3
    )
    assert sleep.call_args_list == [call(0.1), call(0.2)]


@pytest.mark.parametrize(
    "code,level",
    [
        (KafkaError.UNKNOWN_MEMBER_ID, "warning"),
        (KafkaError._ASSIGNMENT_LOST, "warning"),
        (KafkaError.NOT_COORDINATOR, "warning"),
        (KafkaError.TOPIC_AUTHORIZATION_FAILED, "error"),
    ],
)
def test_async_callback_reports_failure_without_retry(avro_consumer, code, level):
    consumer = avro_consumer()
    error, partitions = KafkaError(code), [Mock()]
    with patch("confluent_kafka_helpers.callbacks.logger") as logger, patch.object(
        statsd, "increment"
    ) as increment:
        consumer.config["on_commit"](error, partitions)
    getattr(logger, level).assert_called_once()
    increment.assert_called_once()
    consumer.consumer.commit.assert_not_called()


def test_async_success_preserves_custom_callback(avro_consumer):
    custom, partitions = Mock(), [Mock()]
    consumer = avro_consumer(config_override={"on_commit": custom})
    with patch.object(statsd, "increment") as increment:
        consumer.config["on_commit"](None, partitions)
    custom.assert_called_once_with(None, partitions)
    increment.assert_not_called()


@pytest.mark.parametrize("code", [KafkaError._NO_OFFSET, KafkaError.TOPIC_AUTHORIZATION_FAILED])
def test_propagated_synchronous_errors_do_not_log_errors(avro_consumer, caplog, code):
    consumer = avro_consumer()
    exception = KafkaException(KafkaError(code))
    consumer.consumer.commit.side_effect = exception
    with patch("confluent_kafka_helpers.consumer.logger") as logger, patch(
        "confluent_kafka_helpers.utils.time.sleep"
    ) as sleep:
        with pytest.raises(KafkaException) as raised:
            consumer.commit(asynchronous=False)
    assert raised.value is exception
    assert raised.value.args[0].code() == code
    logger.error.assert_not_called()
    sleep.assert_not_called()
    assert not [record for record in caplog.records if record.levelno >= 40]


@pytest.mark.parametrize("code", [KafkaError.UNKNOWN_MEMBER_ID, KafkaError._NO_OFFSET])
def test_custom_async_callback_owns_reporting(avro_consumer, code):
    custom, partitions = Mock(), [Mock()]
    error = KafkaError(code)
    consumer = avro_consumer(config_override={"on_commit": custom})
    with patch("confluent_kafka_helpers.callbacks.logger") as logger, patch.object(
        statsd, "increment"
    ) as increment:
        consumer.config["on_commit"](error, partitions)
    custom.assert_called_once_with(error, partitions)
    logger.error.assert_not_called()
    logger.warning.assert_not_called()
    increment.assert_called_once_with(
        f"{base_metric}.consumer.commit.count.error", tags=[f"code:{code}"]
    )


def test_async_custom_callback_exception_propagates():
    custom = Mock(side_effect=ValueError("custom"))
    with pytest.raises(ValueError, match="custom"):
        default_on_commit_cb(KafkaError(KafkaError.UNKNOWN_MEMBER_ID), [], custom)
