from collections import defaultdict, deque

from confluent_kafka_helpers.mocks.message import Message

DEFAULT_NUM_PARTITIONS = 8


class KafkaError:
    _PARTITION_EOF = -191


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


@singleton
class InMemoryLog(defaultdict):
    def __init__(self):
        super().__init__(lambda: defaultdict(deque))


class AdminAPI:
    def __init__(self, log: InMemoryLog = InMemoryLog) -> None:
        self._log = log()

    def get_topics(self):
        topics = {
            topic: [key for key, value in partitions.items()]
            for topic, partitions in self._log.items()
        }
        return topics

    def create_topics(self, topics, num_partitions):
        for topic in topics:
            for i in range(num_partitions):
                message_eof = Message(
                    value=None, topic=topic, partition=i,
                    error_code=KafkaError._PARTITION_EOF
                )
                partition_log = self._log[topic][i]
                offset = len(partition_log)
                if offset == 0:
                    partition_log.append(message_eof)


class Broker:
    def __init__(
        self, admin: AdminAPI = AdminAPI, log: InMemoryLog = InMemoryLog
    ) -> None:
        self._consuming_topics_partition = dict()
        self._log = log()
        self._admin = admin()

    @property
    def admin(self):
        return self._admin

    @property
    def num_partitions(self):
        return DEFAULT_NUM_PARTITIONS

    # def consume_topics_partition(self, topics_partition):
    #     topics = [topic.topic for topic in topics_partition]
    #     self._admin.create_topics(topics, num_partitions=DEFAULT_NUM_PARTITIONS)
    #     for topic_partition in topics_partition:
    #         topic, partition = topic_partition.topic, topic_partition.partition
    #         self._consuming_topics_partition[topic] = partition

    def add_message(self, message):
        topic, partition = message.topic(), message.partition()
        offset = len(self._log[topic][partition])
        message._offset = offset
        self._admin.create_topics(
            [topic], num_partitions=DEFAULT_NUM_PARTITIONS
        )
        self._log[topic][partition].append(message)

        return offset

    def prefetch_messages(self):
        # messages = []
        # for topic, partition in self._consuming_topics_partition.items():
        #     topic_partitions = self._log[topic]
        #     if partition:
        #         messages.append([topic_partitions[partition]])
        #     else:
        #         messages.append(
        #             [messages for messages in topic_partitions.values()]
        #         )

        # filtered_log = defaultdict(dict)
        # for topic, partition in self._consuming_topics_partition.items():
        #     if partition:
        #         filtered_log[topic][partition] = self._log[topic][partition]
        #     else:
        #         filtered_log[topic] = self._log[topic]

        messages = []
        for topic, partitions in self._log.items():
            topic_messages = []
            for partition, log in partitions.items():
                # if not log:
                #     message_eof = Message(
                #         value=None, topic=topic, partition=partition, offset=0,
                #         error_code=KafkaError._PARTITION_EOF
                #     )
                #     log.append(message_eof)
                message = (topic, partition, log)
                topic_messages.append(message)
            messages.append(topic_messages)
        import ipdb; ipdb.set_trace()
        return messages


if __name__ == "__main__":
    broker = Broker()
    broker.add_message('1', '2', '3', 0)
    print(broker.admin.get_topics())
