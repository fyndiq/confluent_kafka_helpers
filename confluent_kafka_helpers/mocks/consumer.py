from collections import deque
from typing import NamedTuple

import structlog

from confluent_kafka_helpers.mocks.kafka import Broker, KafkaError
from confluent_kafka_helpers.mocks.message import Message

logger = structlog.get_logger(__name__)


class TopicPartition(NamedTuple):
    topic: str
    partition: int = None
    offset: int = None


class MockConsumer:
    def __init__(self, config, broker: Broker = Broker):
        self._config = config
        self._broker = broker()
        self._message = None
        self._group_id = config.get('group.id')
        self._prefetched_messages = None

    def _poll(self):
        if not self._prefetched_messages:
            logger.debug("Prefetching messages")
            messages = self._broker.poll()
            self._prefetched_messages = messages
        else:
            logger.debug("Using prefetched messages")
            messages = self._prefetched_messages
        return messages

    def poll(self, *args, **kwargs):
        # self._offset_manager.update_offsets()
        # TODO: don't fetch messages from broker every time, save batch locally until all messsages are processed
        # TODO: only return non-committed messages on next _poll
        # TODO: don't use deque, use a flat list of messages and filter committed messages in broker
        messages_batch = self._poll()

        for topic in messages_batch:
            for partition in topic:
                messages = partition.messages
                while messages:
                    message = messages.popleft()
                    self._message = message
                    return message
                else:
                    logger.debug(
                        f"Reached EOF {partition.topic}.{partition.partition}"
                    )
                    del topic[0]
                    return Message(
                        value=None, topic=partition.topic,
                        partition=partition.partition,
                        error_code=KafkaError._PARTITION_EOF
                    )
        else:
            logger.debug("No more messages in this batch")

    def assign(self, topics_partitions):
        self._broker.assign(self._group_id, topics_partitions)

    def subscribe(self, topics):
        self._broker.subscribe(self._group_id, topics)

    def commit(self, *args, **kwargs):
        self._broker.commit(self._group_id, self._message)

    def close(self):
        pass

    def unassign(self):
        pass


class MockAvroConsumer(MockConsumer):
    pass
