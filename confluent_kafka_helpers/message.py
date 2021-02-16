import datetime


def kafka_timestamp_to_datetime(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp / 1000.0) if timestamp is not None else None


def extract_timestamp_from_message(kafka_message):
    timestamp_type, timestamp = kafka_message.timestamp()
    if timestamp_type == 0 or timestamp <= 0:
        timestamp = None
    return timestamp


def decode_kafka_headers(headers):
    if headers and isinstance(headers, list):
        headers = {
            k: v.decode('utf-8') if isinstance(v, bytes) else v for k, v in dict(headers).items()
        }
    else:
        headers = {}
    return headers


class Message:
    __slots__ = ["value", "_raw", "_meta"]

    def __init__(self, kafka_message):
        self.value = kafka_message.value()
        self._raw = kafka_message
        self._meta = MessageMetadata(kafka_message)

    def __repr__(self):
        return f"Message(" f"value={self.value}, " f"_raw={self._raw}, " f"_meta={self._meta}" f")"

    def __bool__(self):
        return True if self.value else False

    def __eq__(self, other):
        return self._raw == other._raw

    def __hash__(self):
        return hash((str(self.value), self._meta.key, self._meta.offset))

    def __lt__(self, other):
        return hash(self) < hash(other)


class MessageMetadata:
    __slots__ = [
        "datetime",
        "headers",
        "key",
        "offset",
        "partition",
        "timestamp",
        "topic",
    ]

    def __init__(self, kafka_message):
        self.key = kafka_message.key()
        self.partition = kafka_message.partition()
        self.offset = kafka_message.offset()
        self.topic = kafka_message.topic()
        self.headers = decode_kafka_headers(kafka_message.headers())
        self.timestamp = extract_timestamp_from_message(kafka_message)
        self.datetime = kafka_timestamp_to_datetime(self.timestamp)

    def __repr__(self):
        return (
            f"MessageMetadata("
            f"key={self.key}, "
            f"partition={self.partition}, "
            f"offset={self.offset}, "
            f"topic={self.topic}, "
            f"headers={self.headers}, "
            f"timestamp={self.timestamp}, "
            f"datetime={self.datetime}"
            f")"
        )
