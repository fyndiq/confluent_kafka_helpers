import enum
from pathlib import Path

from confluent_kafka import avro


class SubjectNameStrategies(enum.Enum):
    TOPICNAME = 'TopicName'
    RECORDNAME = 'RecordName'
    TOPICRECORDNAME = 'TopicRecordName'


class SubjectNameResolver:
    def __init__(self, is_key: bool = False):
        self.is_key = is_key

    @staticmethod
    def factory(strategy: str):
        if strategy == SubjectNameStrategies.TOPICNAME.value:
            return TopicNameStrategyResolver
        elif strategy == SubjectNameStrategies.RECORDNAME.value:
            return RecordNameStrategyResolver
        elif strategy == SubjectNameStrategies.TOPICRECORDNAME.value:
            return TopicRecordNameStrategyResolver
        else:
            raise RuntimeError(f"Invalid subject name strategy: {strategy}")


class TopicNameStrategyResolver(SubjectNameResolver):
    def get_subject(self, schema_file: str) -> str:
        return Path(schema_file).stem

    def get_schema(self, topic: str):
        pass


class RecordNameStrategyResolver(SubjectNameResolver):
    def get_subject(self, schema_file: str) -> str:
        schema = avro.load(schema_file)
        name = schema.name
        return f'{name}-key' if self.is_key else f'{name}-value'

    def get_schema(self, topic):
        pass


class TopicRecordNameStrategyResolver(SubjectNameResolver):
    def get_subject(self, schema_file: str) -> str:
        schema = avro.load(schema_file)
        fullname = schema.fullname
        return f'{fullname}-key' if self.is_key else f'{fullname}-value'

    def get_schema(self, topic):
        pass
