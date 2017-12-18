"""
This module only contains the Message class
definition for yielding in the consumers of
kafka messages.
"""
import datetime


class Message:
    __slots__ = [
        "value", "_raw", "_meta"
    ]

    def __init__(self, kafka_message):
        self.value = kafka_message.value()
        self._raw = kafka_message
        self._meta = MessageMetadata(kafka_message)


def kafka_timestamp_to_datetime(timestamp):
    return datetime.datetime.utcfromtimestamp(
        timestamp / 1000.0
    ) if timestamp is not None else None


def extract_timestamp_from_message(kafka_message):
    timestamp_type, timestamp = kafka_message.timestamp()
    if timestamp_type == 0 or timestamp <= 0:
        timestamp = None
    return timestamp


class MessageMetadata:
    __slots__ = [
        "key", "partition", "offset", "timestamp",
        "datetime", "topic"
    ]

    def __init__(self, kafka_message):
        self.key = kafka_message.key()
        self.partition = kafka_message.partition()
        self.offset = kafka_message.offset()
        self.topic = kafka_message.topic()
        self.timestamp = extract_timestamp_from_message(kafka_message)
        self.datetime = kafka_timestamp_to_datetime(self.timestamp)
