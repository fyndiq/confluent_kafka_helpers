import atexit
import socket
import uuid
import zlib
from collections import defaultdict
from functools import partial
from typing import Callable, List

import structlog
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from confluent_kafka_helpers.consumer import AvroLazyConsumer, get_message
from confluent_kafka_helpers.exceptions import EndOfPartition, KafkaTransportError
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.metrics import base_metric, statsd
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


def find_duplicated_messages(messages, logger=logger):
    """
    Find and log duplicated messages.

    Args:
        messages: List of messages.
    """
    duplicates = defaultdict(list)
    for i, message in enumerate(messages):
        duplicates[message].append(i)

    for message, pos in sorted(duplicates.items()):
        if len(pos) > 1:
            logger.critical("Duplicated messages found", message=message, pos=pos)


def default_error_handler(kafka_error):
    code = kafka_error.code()
    if code == KafkaError._PARTITION_EOF:
        logger.debug("Reached end of partition")
        raise EndOfPartition
    elif code == KafkaError._TRANSPORT:
        statsd.increment(f'{base_metric}.loader.message.count.error')
        raise KafkaTransportError(kafka_error)
    else:
        statsd.increment(f'{base_metric}.loader.message.count.error')
        raise KafkaException(kafka_error)


class MessageGenerator:
    def __init__(
        self, consumer, key, key_filter, error_handler: Callable = default_error_handler
    ) -> None:
        self.consumer = consumer
        self.key = key
        self.key_filter = key_filter
        self.messages: List = []
        self._generator = self._message_generator()

        self._get_message = partial(
            get_message, consumer=consumer, error_handler=error_handler, stop_on_eof=True
        )

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._generator)
        except EndOfPartition:
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.consumer.unassign()
        find_duplicated_messages(self.messages)

    def _message_generator(self):
        while True:
            message = self._get_message()
            if message is None:
                continue

            if self.key_filter(self.key, message.key()):
                self.consumer.decode_message(message)

                message = Message(message)
                # since we use at-least-once message delivery semantics
                # there is a possibility that we read the same message
                # multiple times.
                #
                # so the first step so "solve" this problem is to identify
                # if this is even a problem at all, and too see how often
                # this occurs.
                #
                # if we store all messages we can identify if there are
                # any duplicates.
                #
                # this basically defeats the purpose of having a generator.
                #
                # if we identify that this is an actual problem we should
                # probably remove the generator and return a de-duplicated
                # list instead.
                self.messages.append(message)

                yield message


class AvroMessageLoader:

    DEFAULT_CONFIG = {
        'log.connection.close': False,
        'log.thread.name': False,
        'default.topic.config': {'auto.offset.reset': 'earliest'},
        'fetch.wait.max.ms': 10,
        'fetch.min.bytes': 1000,
        'offset.store.method': 'none',
        'enable.auto.commit': False,
        'fetch.error.backoff.ms': 0,
        'group.id': str(uuid.uuid4()),
        'client.id': socket.gethostname(),
        'enable.partition.eof': True,
    }

    def __init__(self, config):
        self.topic = config['topic']
        self.num_partitions = int(config['num_partitions'])

        default_key_subject_name = f'{self.topic}-key'
        self.key_subject_name = config.get('key_subject_name', default_key_subject_name)

        schema_registry_url = config['consumer']['schema.registry.url']
        schema_registry = AvroSchemaRegistry(schema_registry_url)
        self.key_serializer = partial(
            schema_registry.key_serializer, self.key_subject_name, self.topic
        )

        consumer_config = {**self.DEFAULT_CONFIG, **config['consumer']}
        logger.info("Initializing loader", config=consumer_config)
        self.consumer = AvroLazyConsumer(consumer_config)

        atexit.register(self._close)

    def _close(self):
        logger.info("Closing consumer (loader)")
        self.consumer.close()

    def load(self, key, key_filter=default_key_filter, partitioner=default_partitioner):
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
            KafkaException: Kafka errors.
            KafkaTransportError: Kafka transport errors

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
            "Loading messages from repository",
            topic=self.topic,
            key=key,
            partition_num=partition_num,
        )
        return MessageGenerator(self.consumer, serialized_key, key_filter)
