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

    def __init__(self, loader_config):
        self.topic = loader_config['topic']
        self.key_subject_name = loader_config['key_subject_name']
        self.num_partitions = loader_config['num_partitions']

        schema_registry_url = loader_config['consumer']['schema.registry.url']
        schema_registry = AvroSchemaRegistry(schema_registry_url)

        self.key_serializer = partial(
            schema_registry.key_serializer,
            self.key_subject_name,
            self.topic
        )
        self.consumer = AvroConsumer(loader_config['consumer'])

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
            KafkaException

        Returns:
            list: A list with all stored messages.
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
        _, max_offset = self.consumer.get_watermark_offsets(
            partition, timeout=0.5, cached=False
        )
        logger.info("Loading aggregate from repository",
                    partition=partition, max_offset=max_offset)

        messages = []
        running = True
        try:
            # make sure we don't poll an empty partition since we will
            #     get stuck in an infinite loop
            while running and max_offset != 0:
                message = self.consumer.poll(timeout=0.1)
                if not message:
                    continue

                if message.error():
                    # TODO: investigate why this is not working
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        running = False
                    else:
                        raise KafkaException(message.error())

                _, max_offset = self.consumer.get_watermark_offsets(
                    partition, timeout=0.5, cached=False
                )
                message_key = message.key()
                message_value = message.value()
                message_offset = message.offset()

                if key_filter(key, message_key):
                    logger.info("Loaded message", key=message_key,
                                message=message_value, offset=message_offset)
                    messages.append(message_value)

                # use this "hack" until KafkaError._PARTITION_EOF works
                if message_offset + 1 == max_offset:
                    logger.info("Reached partition EOF")
                    running = False

        except KeyboardInterrupt:
            print("Aborted")

        # We are not able to reuse the Consumer if we close the connection
        # https://github.com/confluentinc/confluent-kafka-python/issues/204
        # consumer.close()

        return messages
