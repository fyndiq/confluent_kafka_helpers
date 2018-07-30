from confluent_kafka_helpers.mocks.kafka import Broker


class Partitions:
    def __init__(self, partitions):
        self._partitions = partitions

    @property
    def partitions(self):
        return self._partitions


class Topics:
    def __init__(self, topics):
        self._topics = topics

    @property
    def topics(self):
        topics = {
            topic: Partitions(partitions)
            for topic, partitions in self._topics.items()
        }
        return topics


class MockAdminClient:
    def __init__(self, config, broker: Broker = Broker) -> None:
        self._broker = broker()

    def poll(self, timeout):
        pass

    def list_topics(self, timeout):
        topics = self._broker.admin.get_topics()
        return Topics(topics)

    def describe_configs(self, resources, request_timeout=None):
        return {}
