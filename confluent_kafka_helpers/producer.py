from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer

from confluent_kafka_helpers import logger
from confluent_kafka_helpers.schema_registry import AvroSchemaRegistry


class AvroProducer(ConfluentAvroProducer):

    DEFAULT_CONFIG = {
        'log.connection.close': False
    }

    def _get_default_subject_names(self, topic):
        """
        Returns the default avro schema names for the topic
        """
        key_subject_name = f'{topic}-key'
        value_subject_name = f'{topic}-value'
        return key_subject_name, value_subject_name

    def _add_topic_data(self, topic, key_subject_name, value_subject_name, schema_registry):
        """
        Adds a topic with the schema names to self.supported_topics
        """
        key_schema = schema_registry.get_latest_schema(key_subject_name)
        value_schema = schema_registry.get_latest_schema(value_subject_name)
        self.supported_topics.append(
            {
                'topic': topic,
                'key_schema': key_schema,
                'value_schema': value_schema
            }
        )

    def _setup_extra_topics(self, topic_list, schema_registry):
        """
        Adds all topics in the list to supported topics with the default
        schema names
        """
        if topic_list is not None:
            for topic in topic_list:
                key_subject_name, value_subject_name = (
                    self._get_default_subject_names(topic)
                )
                self._add_topic_data(
                    topic, key_subject_name, value_subject_name, schema_registry
                )

    def _get_schemas(self, topic):
        for topic_data in self.supported_topics:
            if topic_data['topic'] == topic:
                return topic_data['key_schema'], topic_data['value_schema']

    def __init__(self, config, value_serializer=None):
        config.update(self.DEFAULT_CONFIG)
        schema_registry_url = config['schema.registry.url']
        schema_registry = AvroSchemaRegistry(schema_registry_url)

        self.supported_topics = []

        self.default_topic = config.pop('default_topic')
        default_key_subject_name, default_value_subject_name = (
            self._get_default_subject_names(self.default_topic)
        )

        key_subject_name = config.pop(
            'key_subject_name', default_key_subject_name
        )
        value_subject_name = config.pop(
            'value_subject_name', default_value_subject_name
        )
        self.value_serializer = config.pop(
            'value_serializer', value_serializer
        )
        self._add_topic_data(
            self.default_topic,
            key_subject_name,
            value_subject_name,
            schema_registry
        )

        extra_topics = config.get('extra_topics')
        self._setup_extra_topics(extra_topics, schema_registry)

        # fetch latest schemas from schema registry
        key_schema = schema_registry.get_latest_schema(key_subject_name)
        value_schema = schema_registry.get_latest_schema(value_subject_name)

        super().__init__(config, default_key_schema=key_schema,
                         default_value_schema=value_schema)

    def produce(self, key, value, topic=None, **kwargs):
        if self.value_serializer:
            value = self.value_serializer(value)

        logger.info("Producing message", topic=topic, key=key, value=value)

        if topic is None:
            super().produce(topic=self.default_topic, key=key, value=value, **kwargs)
        else:
            key_schema, value_schema = self._get_schemas(topic)
            super().produce(
                topic=topic,
                key=key,
                value=value,
                key_schema=key_schema,
                value_schema=value_schema,
                **kwargs
            )


