import socket

import structlog
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.avro import Consumer

from confluent_kafka_helpers.callbacks import (
    default_error_cb, default_stats_cb, get_callback)
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.metrics import base_metric, statsd

logger = structlog.get_logger(__name__)



class AvroConsumerLazyDecode(Consumer):
    def poll(self, timeout=None):
        """
        This is an overriden method from confluent_kafka.Consumer class. This handles message
        deserialization using avro schema

        @:param timeout
        @:return message object with deserialized key and value as dict objects
        """
        if timeout is None:
            timeout = -1
        message = super(AvroConsumerLazyDecode, self).poll(timeout)
        if message is None:
            return None
        if not message.value() and not message.key():
            return message
        if not message.error():
            # We are not decoding the message value now, because we filter
            # the messages by key in the AvroMessageLoader

            #if message.value() is not None:
            #    decoded_value = self._serializer.decode_message(message.value())
            #    message.set_value(decoded_value)

            if message.key() is not None:
                decoded_key = self._serializer.decode_message(message.key())
                message.set_key(decoded_key)
        return message

    def decode_message_value(self, message):
        decoded_value = self._serializer.decode_message(message.value())
        message.set_value(decoded_value)


class AvroConsumer:

    DEFAULT_CONFIG = {
        'log.connection.close': False,
        'log.thread.name': False,
        'enable.auto.commit': False,
        'default.topic.config': {
            'auto.offset.reset': 'latest'
        },
        'fetch.wait.max.ms': 10,
        'fetch.error.backoff.ms': 0,
        'fetch.message.max.bytes': 10500,
        'session.timeout.ms': 6000,
        'api.version.request': True,
        'statistics.interval.ms': 15000,
        'client.id': socket.gethostname()
    }

    def __init__(self, config):
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

        logger.info("Initializing consumer", config=self.config)
        self.consumer = ConfluentAvroConsumer(self.config)
        self.consumer.subscribe(self.topics)

    def __getattr__(self, name):
        return getattr(self.consumer, name)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._message_generator())

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
                        raise StopIteration
                    else:
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
