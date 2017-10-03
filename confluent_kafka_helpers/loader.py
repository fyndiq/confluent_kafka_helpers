import zlib
from functools import partial

from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.avro import AvroConsumer
from confluent_kafka_helpers import logger
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry


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


class AvroMessageLoader:

    DEFAULT_CONFIG = {
        'default.topic.config': {
            'auto.offset.reset': 'earliest'
        }
    }

    def __init__(self, config):
        self.topic = config['topic']
        self.num_partitions = config['num_partitions']

        default_key_subject_name = f'{self.topic}-key'
        self.key_subject_name = config.get(
            'key_subject_name', default_key_subject_name
        )

        schema_registry_url = config['consumer']['schema.registry.url']
        schema_registry = AvroSchemaRegistry(schema_registry_url)

        self.key_serializer = partial(
            schema_registry.key_serializer, self.key_subject_name, self.topic
        )

        consumer_config = config['consumer']
        consumer_config.update(self.DEFAULT_CONFIG)
        self.consumer = AvroConsumer(consumer_config)

    def load(self, key, key_filter=default_key_filter,
             partitioner=default_partitioner):
        """
        Load all stored messages for the given key.

        Args:
            key: Key used when the event was stored, probably the
                ID of the message.
            key_filter: Callable used to filter the key. Usually we
                are only interested in messages with the same keys.
            partitioner: Callable used to calculate which partition
                the message was stored in when produced.

        Raises:
            KafkaException: On unexpected Kafka errors

        Returns:
            list: A list with all messages for the given key.
        """
        # since all messages with the same key are guaranteed to be stored
        #    in the same topic partition (using default partitioner) we can
        #    optimize the loading by only reading from that specific partition.
        #
        # if we know the key and total number of partitions we can
        #     deterministically calculate the partition number that was used.
        serialized_key = self.key_serializer(key)
        partition_num = partitioner(serialized_key, self.num_partitions)
        partition = TopicPartition(self.topic, partition_num, 0)

        self.consumer.assign([partition])
        min_offset, max_offset = self.consumer.get_watermark_offsets(
            partition, timeout=1.0, cached=False
        )
        logger.info(
            "Loading messages from repository", topic=self.topic, key=key,
            partition_num=partition_num, min_offset=min_offset,
            max_offset=max_offset
        )

        messages = []
        try:
            while True and max_offset != 0:
                message = self.consumer.poll(timeout=0.1)
                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached EOF")
                        break
                    else:
                        raise KafkaException(message.error())

                message_key, message_value, message_offset = (
                    message.key(), message.value(), message.offset()
                )
                if key_filter(key, message_key):
                    logger.debug(
                        "Loading message", key=message_key,
                        message=message_value, offset=message_offset
                    )
                    messages.append(message_value)

        except KeyboardInterrupt:
            print("Aborted")

        return messages
