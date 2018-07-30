from functools import lru_cache
from typing import Callable

from confluent_kafka.admin import RESOURCE_TOPIC
from confluent_kafka.admin import AdminClient as ConfluentAdminClient
from confluent_kafka.admin import ConfigResource

from confluent_kafka_helpers.config import get_bootstrap_servers
from confluent_kafka_helpers.mocks.adminapi import MockAdminClient


class AdminClient:
    def __init__(
        self, config, client: ConfluentAdminClient = ConfluentAdminClient,
        mock_client: MockAdminClient = MockAdminClient,
        get_bootstrap_servers: Callable = get_bootstrap_servers
    ) -> None:
        bootstrap_servers, enable_mock = get_bootstrap_servers(config)
        client_cls = mock_client if enable_mock else client
        self._client = client_cls({'bootstrap.servers': bootstrap_servers})

    @lru_cache(maxsize=128)
    def list_topics(self, timeout=1.0):
        self._client.poll(timeout=timeout)
        topics = self._client.list_topics(timeout=timeout)
        return topics.topics

    @lru_cache(maxsize=128)
    def get_num_partitions(self, topic):
        topics = self.list_topics()
        return len(topics[topic].partitions)

    def get_topics_config(self, topics, timeout=1.0):
        resources = [ConfigResource(RESOURCE_TOPIC, topic) for topic in topics]
        fs = self._client.describe_configs(resources, request_timeout=1.0)
        for res, f in fs.items():
            configs = f.result()
            yield configs.values()


if __name__ == "__main__":
    # config = {'bootstrap.servers': 'localhost:9094'}
    config = {'bootstrap.servers': 'mock://localhost:9094'}
    client = AdminClient(config)
    client._client._broker.admin.create_topics(['foo.foo.commands'], 8)
    print(client.list_topics())
    print(client.get_num_partitions('foo.foo.commands'))
    configs = client.get_topics_config(['foo.foo.commands', '_schemas'])
    for config in configs:
        print(config)
