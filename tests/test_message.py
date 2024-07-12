"""
Test messages are properly turned into a Message object
"""

import datetime
from unittest.mock import Mock

import pytest

from confluent_kafka_helpers.message import (
    Message,
    extract_timestamp_from_message,
    kafka_timestamp_to_datetime,
)

mock_message = Mock()
mock_message.value = Mock(return_value={"data": "test"})
mock_message.key = Mock(return_value="test")
mock_message.partition = Mock(return_value=2)
mock_message.offset = Mock(return_value=100)
mock_message.topic = Mock(return_value="testing_topic")
mock_message.timestamp = Mock(return_value=(0, 0))
mock_message.headers = Mock(return_value=None)


def test_timestamp_and_datetime_extraction():
    """
    tests that the function that extracts the timestamp
    and datetime from a kafka message is working correctly
    """
    test_datetime = datetime.datetime(2017, 1, 15)
    test_timestamp = (test_datetime - datetime.datetime(1970, 1, 1)).total_seconds() * 1000.0

    # valid timestamp in the form kafka would send it if SET
    mock_message.timestamp = Mock(return_value=(1, test_timestamp))

    timestamp = extract_timestamp_from_message(mock_message)
    assert timestamp == test_timestamp
    assert kafka_timestamp_to_datetime(timestamp) == test_datetime

    # valid timestamp in the form kafka would send it if NOT SET
    mock_message.timestamp = Mock(return_value=(1, -1))

    timestamp = extract_timestamp_from_message(mock_message)
    assert timestamp is None
    assert kafka_timestamp_to_datetime(timestamp) is None

    # no timestamp in the form kafka would send it if NOT AVAILABLE
    mock_message.timestamp = Mock(return_value=(0, 0))

    timestamp = extract_timestamp_from_message(mock_message)
    assert timestamp is None
    assert kafka_timestamp_to_datetime(timestamp) is None


def test_message_object():
    """
    tests to ensure that when handed an object with all of the
    kafka message methods, the new Message object has the correct
    attributes set.
    """

    # valid timestamp
    mock_message.timestamp = Mock(
        return_value=(
            1,
            (datetime.datetime(2017, 1, 15) - datetime.datetime(1970, 1, 1)).total_seconds()
            * 1000.0,
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
    mock_message.timestamp = Mock(return_value=(1, -1))
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
    mock_message.timestamp = Mock(return_value=(0, 0))
    new_message = Message(mock_message)

    assert new_message.value == mock_message.value()
    assert new_message._raw == mock_message
    assert new_message._meta.key == mock_message.key()
    assert new_message._meta.partition == mock_message.partition()
    assert new_message._meta.offset == mock_message.offset()
    assert new_message._meta.topic == mock_message.topic()
    assert new_message._meta.timestamp is None
    assert new_message._meta.datetime is None


@pytest.mark.parametrize(
    "headers, expected_headers",
    [
        (
            [('foo', b'bar')],
            {'foo': 'bar'},
        ),
        (
            None,
            {},
        ),
        (
            {'foo': b'bar'},
            {},
        ),
        (
            [('foo', None)],
            {'foo': None},
        ),
        (
            [('foo', 1)],
            {'foo': 1},
        ),
    ],
)
def test_headers(headers, expected_headers):
    mock_message.headers = Mock(return_value=headers)
    new_message = Message(mock_message)
    assert new_message._meta.headers == expected_headers
