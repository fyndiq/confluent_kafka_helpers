from dataclasses import dataclass


@dataclass
class KafkaError:
    _code: int

    def code(self):
        return self._code


@dataclass
class KafkaMessage:
    _error: bool = False

    def error(self):
        return self._error
