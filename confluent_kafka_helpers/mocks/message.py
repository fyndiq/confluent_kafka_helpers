import time


class Error:
    def __init__(self, code):
        self._code = code

    def __repr__(self):
        return str(self._code)

    def code(self):
        return self._code


class Message:
    def __init__(
        self, value, topic, partition, key=None, offset=None, error_code=None
    ):
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = Error(error_code) if error_code else None

    def __repr__(self):
        return (
            f"KafkaMessage("
            f"value={self._value}, key={self._key}, "
            f"partition={self._partition}, offset={self._offset}, "
            f"error={self._error})"
        )

    def error(self):
        return self._error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def offset(self):
        return self._offset

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def timestamp(self):
        return (0, int(time.time()))
