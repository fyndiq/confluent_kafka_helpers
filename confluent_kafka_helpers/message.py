"""
This module only contains the Message class
definition for yielding in the consumers of
kafka messages.
"""
import datetime


def kafka_timestamp_to_datetime(timestamp):
    return datetime.datetime.utcfromtimestamp(
        timestamp / 1000.0
    ) if timestamp is not None else None


def extract_timestamp_from_message(kafka_message):
    timestamp_type, timestamp = kafka_message.timestamp()
    if timestamp_type == 0 or timestamp <= 0:
        timestamp = None
    return timestamp


class Message:
    __slots__ = ["value", "_raw", "_meta"]

    def __init__(self, kafka_message):
        self.value = kafka_message.value()
        self._raw = kafka_message
        self._meta = MessageMetadata(kafka_message)

    def __repr__(self):
        return f"Message(value={self.value})"

    def __bool__(self):
        return True if self.value else False

    def __eq__(self, other):
        return self._raw == other._raw

    def __hash__(self):
        return hash((str(self.value), self._meta.key, self._meta.timestamp))

    def __lt__(self, other):
        return hash(self) < hash(other)


class MessageMetadata:
    __slots__ = ["key", "partition", "offset", "timestamp", "datetime", "topic"]

    def __init__(self, kafka_message):
        self.key = kafka_message.key()
        self.partition = kafka_message.partition()
        self.offset = kafka_message.offset()
        self.topic = kafka_message.topic()
        self.timestamp = extract_timestamp_from_message(kafka_message)
        self.datetime = kafka_timestamp_to_datetime(self.timestamp)

    def __repr__(self):
        msg = (
            f"MessageMeta(key={self.key}, partition={self.partition}, "
            f"offset={self.offset}, topic={self.topic}, "
            f"timestamp={self.timestamp}, datetime={self.datetime})"
        )
        return msg
