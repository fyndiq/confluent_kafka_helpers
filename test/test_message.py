"""
Test messages are properly turned into a Message object
"""
from unittest.mock import Mock
import datetime

from confluent_kafka_helpers.message import Message


mock_message = Mock()
mock_message.value = Mock(return_value={"data": "test"})
mock_message.key = Mock(return_value="test")
mock_message.partition = Mock(return_value=2)
mock_message.offset = Mock(return_value=100)
mock_message.topic = Mock(return_value="testing_topic")


def test_message_object():
    """
    tests to ensure that when handed an object with all of the
    kafka message methods, the new Message object has the correct
    attributes set.
    """

    # valid timestamp
    mock_message.timestamp = Mock(
        return_value=(
            1, (datetime.datetime(2017, 1, 15) - datetime.datetime(1970, 1, 1)).total_seconds()*1000.0
        )
    )

    new_message = Message(mock_message)

    assert new_message.value == mock_message.value()
    assert new_message._raw == mock_message
    assert new_message._meta.key == mock_message.key()
    assert new_message._meta.partition == mock_message.partition()
    assert new_message._meta.offset == mock_message.offset()
    assert new_message._meta.topic == mock_message.topic()
    assert new_message._meta.timestamp == mock_message.timestamp()[1]

    assert new_message._meta.datetime == datetime.datetime(2017, 1, 15)


def test_timestamp_is_negative():
    """
    If the timestamp is negative (not set by kafka),
    it should be reflected as None in the message metadata
    """
    mock_message.timestamp = Mock(
        return_value=(1, -1)
    )
    new_message = Message(mock_message)

    assert new_message.value == mock_message.value()
    assert new_message._raw == mock_message
    assert new_message._meta.key == mock_message.key()
    assert new_message._meta.partition == mock_message.partition()
    assert new_message._meta.offset == mock_message.offset()
    assert new_message._meta.topic == mock_message.topic()
    assert new_message._meta.timestamp is None
    assert new_message._meta.datetime is None


def test_timestamp_is_not_available():
    """
    If the timestamp is 0 and not available,
    it should be reflected as None in the message metadata
    """
    mock_message.timestamp = Mock(
        return_value=(0, 0)
    )
    new_message = Message(mock_message)

    assert new_message.value == mock_message.value()
    assert new_message._raw == mock_message
    assert new_message._meta.key == mock_message.key()
    assert new_message._meta.partition == mock_message.partition()
    assert new_message._meta.offset == mock_message.offset()
    assert new_message._meta.topic == mock_message.topic()
    assert new_message._meta.timestamp is None
    assert new_message._meta.datetime is None
