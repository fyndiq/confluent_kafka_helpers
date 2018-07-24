import time
from collections import defaultdict, deque


class Borg:
    __shared_state = {}

    def __init__(self):
        self.__dict__ = self.__shared_state


class Error:
    def code(self):
        return None


class Message:
    def __init__(self, value, key, topic, partition):
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition

    def error(self):
        return Error()

    def value(self):
        return self._value

    def key(self):
        return self._key

    def offset(self):
        pass

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def timestamp(self):
        return (0, int(time.time()))


class Broker(Borg):
    def __init__(self):
        super().__init__()
        self._log = defaultdict(lambda: defaultdict(deque))
        import ipdb; ipdb.set_trace()

    def send_message(self, value, key, topic, partition):
        message = Message(value, key, topic, partition)
        self._log[topic][partition].append(message)
        import ipdb; ipdb.set_trace()

    def get_message(self):
        import ipdb; ipdb.set_trace()
        return None
