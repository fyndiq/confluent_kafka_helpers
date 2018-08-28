import socket
from collections import defaultdict
from itertools import chain

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer

from confluent_kafka_helpers.adminapi import AdminClient
from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_stats_cb, get_callback
)
from confluent_kafka_helpers.config import get_bootstrap_servers
from confluent_kafka_helpers.exceptions import EndOfPartition
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.metrics import base_metric, statsd
from confluent_kafka_helpers.mocks.consumer import (
    MockAvroConsumer, MockConsumer
)

logger = structlog.get_logger(__name__)


def check_eof(topic_partition_eof_map):
    partitions_eof = chain(
        *[
            list(partitions.values())
            for topic, partitions in topic_partition_eof_map.items()
        ]
    )
    return all(partitions_eof)


class AvroConsumer:

    DEFAULT_CONFIG = {
        'api.version.request': True,
        'client.id': socket.gethostname(),
        'default.topic.config': {
            'auto.offset.reset': 'latest'
        },
        'enable.auto.commit': False,
        'fetch.error.backoff.ms': 0,
        'fetch.wait.max.ms': 10,
        'log.connection.close': False,
        'log.thread.name': False,
        'session.timeout.ms': 6000,
        'statistics.interval.ms': 15000
    }

    def __init__(
        self, config,
        avro_consumer: ConfluentAvroConsumer = ConfluentAvroConsumer,
        mock_avro_consumer: MockAvroConsumer = MockAvroConsumer,
        admin_client: AdminClient = AdminClient
    ) -> None:
        self.non_blocking = config.pop('non_blocking', False)
        self.stop_on_eof = config.pop('stop_on_eof', False)
        self.poll_timeout = config.pop('poll_timeout', 0.1)

        self.config = {**self.DEFAULT_CONFIG, **config}
        self.config['error_cb'] = get_callback(
            config.pop('error_cb', None), default_error_cb
        )
        self.config['stats_cb'] = get_callback(
            config.pop('stats_cb', None), default_stats_cb
        )
        self.topics = self._get_topics(self.config)

        bootstrap_servers, enable_mock = get_bootstrap_servers(config)
        consumer_cls = mock_avro_consumer if enable_mock else avro_consumer

        logger.info(
            "Initializing consumer", config=self.config, enable_mock=enable_mock
        )
        self.consumer = consumer_cls(self.config)
        self.consumer.subscribe(self.topics)

        self.topic_partition_eof_map = defaultdict(dict)
        if self.stop_on_eof:
            admin_client = admin_client(config)
            for topic in self.topics:
                num_partitions = admin_client.get_num_partitions(topic)
                for i in range(num_partitions):
                    self.topic_partition_eof_map[topic][i] = False

    def __getattr__(self, name):
        return getattr(self.consumer, name)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._message_generator())
        except EndOfPartition:
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        # the only reason a consumer exits is when an
        # exception is raised.
        #
        # close down the consumer cleanly accordingly:
        #  - stops consuming
        #  - commit offsets (only on auto commit)
        #  - leave consumer group
        logger.info("Closing consumer")
        self.consumer.close()

    def _message_generator(self):
        while True:
            message = self.consumer.poll(timeout=self.poll_timeout)
            if message is None:
                if self.non_blocking:
                    yield None
                continue

            statsd.increment(f'{base_metric}.consumer.message.count.total')
            if message.error():
                error_code = message.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    if self.stop_on_eof:
                        self.topic_partition_eof_map[message.topic()][
                            message.partition()
                        ] = True
                        eof = check_eof(self.topic_partition_eof_map)
                        if eof:
                            raise EndOfPartition
                    continue
                else:
                    statsd.increment(
                        f'{base_metric}.consumer.message.count.error'
                    )
                    raise KafkaException(message.error())

            yield Message(message)

    def _get_topics(self, config):
        topics = config.pop('topics', None)
        assert topics is not None, "You must subscribe to at least one topic"

        if not isinstance(topics, list):
            topics = [topics]

        return topics

    @property
    def is_auto_commit(self):
        return self.config.get('enable.auto.commit', True)


class AvroLazyConsumer:
    """
    By default the Confluent AvroConsumer decode all messages in the partition.

    This consumer uses a lazy approach, it doesn't decode the messages, just
    provide the methods so we can do it manually.

    We use this approach, because we want to check the key messages before
    decoding the message, this will avoid performance issues.
    """

    def __init__(
        self, config,
        consumer: Consumer = Consumer,
        mock_consumer: MockConsumer = MockConsumer,
        avro_consumer: ConfluentAvroConsumer = ConfluentAvroConsumer,
        mock_avro_consumer: MockAvroConsumer = MockAvroConsumer
    ) -> None:
        bootstrap_servers, enable_mock = get_bootstrap_servers(config)
        avro_consumer_cls = mock_avro_consumer if enable_mock else avro_consumer
        consumer_cls = mock_consumer if enable_mock else consumer
        self._avro_consumer = avro_consumer_cls(config)
        self._consumer = consumer_cls

    def __getattr__(self, name):
        return getattr(self._avro_consumer, name)

    def poll(self, timeout=None):
        if timeout is None:
            timeout = -1

        # We use the Consumer.poll because we want to avoid the
        # ConfluentAvroConsumer.poll, the later is doing the decode
        # of all the messages and we want to have a lazy approach
        message = self._consumer.poll(self, timeout)
        return message

    def decode_message(self, message):
        if not message.error():
            if message.value() is not None:
                decoded_value = self._serializer.decode_message(message.value())
                message.set_value(decoded_value)

            if message.key() is not None:
                decoded_key = self._serializer.decode_message(message.key())
                message.set_key(decoded_key)
        return message
