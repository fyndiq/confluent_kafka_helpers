import atexit
import socket

import structlog
from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers.callbacks import (
    default_error_cb,
    default_on_delivery_cb,
    default_stats_cb,
    get_callback,
)
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry, SchemaNotFound
from confluent_kafka_helpers.tracing import tags, tracer

logger = structlog.get_logger(__name__)


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """

    pass


class AvroProducer(ConfluentAvroProducer):
    DEFAULT_CONFIG = {
        'client.id': socket.gethostname(),
        'log.connection.close': False,
        'enable.idempotence': True,
        'max.in.flight': 1,
        'linger.ms': 1000,
    }

    def __init__(
        self,
        config,
        value_serializer=None,
        schema_registry=AvroSchemaRegistry,
        get_callback=get_callback,
        **kwargs,
    ):
        config = {**self.DEFAULT_CONFIG, **config}
        config['on_delivery'] = get_callback(
            config.pop('on_delivery', None), default_on_delivery_cb
        )
        config['error_cb'] = get_callback(config.pop('error_cb', None), default_error_cb)
        config['stats_cb'] = get_callback(config.pop('stats_cb', None), default_stats_cb)

        schema_registry_url = config['schema.registry.url']
        self.schema_registry = schema_registry(schema_registry_url)
        self.value_serializer = config.pop('value_serializer', value_serializer)

        topics = config.pop('topics')
        self.topic_schemas = self._get_topic_schemas(topics)

        # use the first topic as default
        default_topic_schema = next(iter(self.topic_schemas.values()))
        self.default_topic, *_ = default_topic_schema

        logger.info("Initializing producer", config=config)
        atexit.register(self._close)

        super().__init__(config, **kwargs)

    def _close(self):
        logger.info("Flushing producer")
        super().flush()

    def _get_subject_names(self, topic):
        """
        Get subject names for given topic.
        """
        key_subject_name = f'{topic}-key'
        value_subject_name = f'{topic}-value'
        return key_subject_name, value_subject_name

    def _get_topic_schemas(self, topics):
        """
        Get schemas for all topics.
        """
        topic_schemas = {}
        for topic in topics:
            key_name, value_name = self._get_subject_names(topic)
            try:
                key_schema = self.schema_registry.get_latest_schema(key_name)
            except SchemaNotFound:
                # topics that are used for only pub/sub will probably not
                # have a key set on the messages.
                #
                # on these topics we should not require a key schema.
                key_schema = None
            value_schema = self.schema_registry.get_latest_schema(value_name)
            topic_schemas[topic] = (topic, key_schema, value_schema)

        return topic_schemas

    def produce(self, value, key=None, topic=None, headers=None, **kwargs):
        if headers is None:
            headers = {}
        topic = topic or self.default_topic
        try:
            _, key_schema, value_schema = self.topic_schemas[topic]
        except KeyError:
            raise TopicNotRegistered(f"Topic {topic} is not registered")

        if self.value_serializer:
            value = self.value_serializer(value)

        logger.info("Producing message", topic=topic, key=key, value=value, headers=headers)
        with tracer.inject_headers_and_start_span(
            operation_name='kafka.producer.produce', headers=headers
        ) as span:
            span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_PRODUCER)
            span.set_tag(tags.MESSAGE_BUS_DESTINATION, topic)
            span.set_tag(tags.MESSAGE_BUS_KEY, key)
            super().produce(
                topic=topic,
                key=key,
                value=value,
                key_schema=key_schema,
                value_schema=value_schema,
                headers=headers,
                **kwargs,
            )
