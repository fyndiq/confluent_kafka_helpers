import uuid
import zlib
from functools import partial

import structlog
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.avro import AvroConsumer

from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry

logger = structlog.get_logger(__name__)


def default_partitioner(key, num_partitions):
    """
    Algorithm used in Kafkas default 'consistent' partitioner
    """
    return zlib.crc32(key) % num_partitions


def default_key_filter(key, message_key):
    """
    Only load messages if the condition is true.

    Default we are only interested in keys that belong
    to the same aggregate.
    """
    return key == message_key


class MessageGenerator:
    def __init__(self, consumer, key, key_filter):
        self.consumer = consumer
        self.key = key
        self.key_filter = key_filter

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._message_generator())

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.consumer.unassign()

    def _message_generator(self):
        message = self.consumer.poll(timeout=0.1)
        if message is None:
            yield None

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Reached EOF")
                raise StopIteration
            else:
                raise KafkaException(message.error())

        message_key, message_value = (message.key(), message.value())
        if self.key_filter(self.key, message_key):
            yield message_value


class AvroMessageLoader:

    DEFAULT_CONSUMER_CONFIG = {
        'log.connection.close': False,
        'log.thread.name': False,
        'default.topic.config': {
            'auto.offset.reset': 'earliest'
        },
        'fetch.wait.max.ms': 10,
        'offset.store.method': 'none',
        'enable.auto.commit': False,
        'fetch.error.backoff.ms': 0,
        'session.timeout.ms': 6000,
        'group.id': str(uuid.uuid4()),
        'api.version.request': True
    }

    def __init__(self, config):
        self.topic = config['topic']
        self.num_partitions = int(config['num_partitions'])

        default_key_subject_name = f'{self.topic}-key'
        self.key_subject_name = config.get(
            'key_subject_name', default_key_subject_name
        )

        schema_registry_url = config['consumer']['schema.registry.url']
        schema_registry = AvroSchemaRegistry(schema_registry_url)

        self.key_serializer = partial(
            schema_registry.key_serializer, self.key_subject_name, self.topic
        )
        consumer_config = {**self.DEFAULT_CONSUMER_CONFIG, **config['consumer']}
        self.consumer = AvroConsumer(consumer_config)

    def __del__(self):
        # TODO: use atexit
        try:
            self.consumer.close()
        except AttributeError:
            pass

    def load(self, key, key_filter=default_key_filter,
             partitioner=default_partitioner):  # yapf: disable
        """
        Load all messages from a topic for the given key.

        Args:
            key: Key used when the message was stored, probably the
                ID of the message.
            key_filter: Callable used to filter the key. Usually we
                are only interested in messages with the same key.
            partitioner: Callable used to calculate which partition
                the message was stored on when it was produced.

        Raises:
            KafkaException: On unexpected Kafka errors

        Returns:
            MessageGenerator: A generator that yields messages.
        """
        # since all messages with the same key are guaranteed to be stored
        # in the same topic partition (using default partitioner) we can
        # optimize the loading by only reading from that specific partition.
        #
        # if we know the key and total number of partitions we can
        # deterministically calculate the partition number that was used.
        serialized_key = self.key_serializer(key)
        partition_num = partitioner(serialized_key, self.num_partitions)
        # TODO: cache min offset for each key
        partition = TopicPartition(self.topic, partition_num, 0)

        self.consumer.assign([partition])
        logger.info(
            "Loading messages from repository", topic=self.topic, key=key,
            partition_num=partition_num
        )
        return MessageGenerator(self.consumer, key, key_filter)
