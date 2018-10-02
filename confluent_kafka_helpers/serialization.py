from enum import Enum

from confluent_kafka import avro
from confluent_kafka.avro import CachedSchemaRegistryClient, MessageSerializer


class SubjectNameStrategy(Enum):
    TopicNameStrategy = 0
    RecordNameStrategy = 1
    TopicRecordNameStrategy = 2


class TopicNotRegistered(Exception):
    """
    Raised when someone tries to produce with a topic that
    hasn't been registered in the 'topics' configuration.
    """
    pass


class Serializer:
    """
    Base class for all key and value serializers.
    This default implementation returns the value intact.
    """
    def __init__(self, config, **kwargs):
        pass

    def serialize(self, value, topic, **kwargs):
        return value

    # Producer can instantiate multiple serializers (at least two),
    # and config cleanup could not/should not be done from serializer __init__
    # At the same time, config should be cleaned up before instantiating
    # C producer implementation, otherwise it will throw something like
    # cimpl.KafkaException: KafkaError{code=_INVALID_ARG,val=-186 ...
    def config_keys(self) -> list:
        return []


class AvroSerializer(Serializer):
    DEFAULT_CONFIG = {
        'auto.register.schemas': False,
        'key.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy,
        'value.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy
    }

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        config = {**self.DEFAULT_CONFIG, **config}
        schema_registry_url = config['schema.registry.url']
        self.schema_registry = CachedSchemaRegistryClient(schema_registry_url)
        self.auto_register_schemas = config['auto.register.schemas']
        self.key_subject_name_strategy = config['key.subject.name.strategy']
        self.value_subject_name_strategy = config['value.subject.name.strategy']
        self._serializer_impl = MessageSerializer(self.schema_registry)

    def _get_subject(self, topic, schema, strategy, is_key=False):
        if strategy == SubjectNameStrategy.TopicNameStrategy:
            subject = topic
        elif strategy == SubjectNameStrategy.RecordNameStrategy:
            subject = schema.fullname
        elif strategy == SubjectNameStrategy.TopicRecordNameStrategy:
            subject = '%{}-%{}'.format(topic, schema.fullname)
        else:
            raise ValueError('Unknown SubjectNameStrategy')

        subject += '-key' if is_key else '-value'
        return subject

    def _ensure_schema(self, topic, schema, is_key=False):
        subject = self._get_subject(
            topic, schema, self.key_subject_name_strategy if is_key else self.value_subject_name_strategy, is_key)

        if self.auto_register_schemas:
            schema_id = self.schema_registry.register(subject, schema)
            schema = self.schema_registry.get_by_id(schema_id)
        else:
            schema_id, schema, _ = self.schema_registry.get_latest_schema(
                subject)

        return schema_id, schema

    def serialize(self, value, topic, is_key=False, **kwargs):
        schema_id, _ = self._ensure_schema(topic, value._schema, is_key)
        return self._serializer_impl.encode_record_with_schema_id(
            schema_id, value, is_key)

    def config_keys(self) -> list:
        return list(self.DEFAULT_CONFIG.keys()) + ['schema.registry.url']


class AvroStringKeySerializer(AvroSerializer):
    """
    A specialized serializer for generic String keys,
    serialized with a simple value avro schema.
    """

    KEY_SCHEMA = avro.loads("""{"type": "string"}""")

    def serialize(self, value, topic, is_key=False, **kwargs):
        schema = self.KEY_SCHEMA if is_key else value._schema
        schema_id, _ = self._ensure_schema(topic, schema, is_key)
        return self._serializer_impl.encode_record_with_schema_id(
            schema_id, value, is_key)
