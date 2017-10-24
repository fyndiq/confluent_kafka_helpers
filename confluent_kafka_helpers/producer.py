from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers import logger
from confluent_kafka_helpers.schema_registry import (
    AvroSchemaRegistry, SchemaNotFound)


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """
    pass


class AvroProducer(ConfluentAvroProducer):

    DEFAULT_CONFIG = {'log.connection.close': False}

    def __init__(self, config, value_serializer=None,
                 schema_registry=AvroSchemaRegistry):  # yapf: disable
        config = {**self.DEFAULT_CONFIG, **config}

        schema_registry_url = config['schema.registry.url']
        self.schema_registry = schema_registry(schema_registry_url)
        self.value_serializer = config.pop('value_serializer', value_serializer)

        topics = config.pop('topics')
        self.topic_schemas = self._get_topic_schemas(topics)

        # use the first topic as default
        default_topic_schema = next(iter(self.topic_schemas.values()))
        self.default_topic, *_ = default_topic_schema

        super().__init__(config)

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

    def produce(self, value, key=None, topic=None, **kwargs):
        topic = topic or self.default_topic
        try:
            _, key_schema, value_schema = self.topic_schemas[topic]
        except KeyError:
            raise TopicNotRegistered(f"Topic {topic} is not registered")

        if self.value_serializer:
            value = self.value_serializer(value)

        logger.info("Producing message", topic=topic, key=key, value=value)
        super().produce(
            topic=topic, key=key, value=value, key_schema=key_schema,
            value_schema=value_schema, **kwargs
        )
