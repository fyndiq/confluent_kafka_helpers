import uuid

from avro.schema import Schema
from confluent_kafka import avro

from confluent_kafka_helpers.producer import Producer
from confluent_kafka_helpers.serialization import AvroStringKeySerializer, \
    AvroSerializer, SubjectNameStrategy

PRODUCER_CONFIG = {
    'bootstrap.servers': 'localhost:29092',
    'schema.registry.url': 'http://localhost:28081',
    'topics': ['test.test.events', 'test.test.commands'],
    'key.serializer': AvroStringKeySerializer,
    'key.subject.name.strategy': SubjectNameStrategy.TopicNameStrategy,
    'value.serializer': AvroSerializer,
    'value.subject.name.strategy': SubjectNameStrategy.RecordNameStrategy,
    'auto.register.schemas': True
}

producer = Producer(PRODUCER_CONFIG)


class Message(dict):
    _schema: Schema = None


value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""


class TestEvent(Message):
    _schema = avro.loads(value_schema_str)


message = TestEvent({
    'name': 'some name'
})


producer.produce(message, key=str(uuid.uuid4()))
