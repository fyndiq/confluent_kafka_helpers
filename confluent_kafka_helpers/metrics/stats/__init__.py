import json

from confluent_kafka_helpers.metrics.stats.brokers import Brokers
from confluent_kafka_helpers.metrics.stats.topics import Topics


class StatsParser:
    def __init__(self, stats):
        self._stats = json.loads(stats)
        self._brokers = Brokers(self._stats['brokers'])
        self._topics = Topics(self._stats['topics'])

    def __getattr__(self, attr):
        return self._stats.get(attr)

    @property
    def brokers(self):
        return self._brokers

    @property
    def topics(self):
        return self._topics
