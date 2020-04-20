#!/usr/bin/env python3
"""
Schema registry client.

Usage:
  schema-registry <command> (-s <subject> -f <file> | --automatic [--folder=<folder>]
                            [--key-strategy=<strategy>] [--value-strategy=<strategy>]
                            [--topic=<topic>]) [--url=<url>] [--test]

Available commands:
    register                   Register a schema to schema registry.
    test                       Test schema compatibility against latest version.

Options:
  -s --subject=<subject>       Schema subject name.
  -f --file=<file>             Avro schema file (*.avsc).
  --url=<url>                  Schema registry URL [default: http://localhost:8081].
  --automatic                  Automatic find schemas in given folder [default: False].
  --folder=<folder>            Folder with Avro schemas [default: schemas].
  --key-strategy=<strategy>    Key subject name strategy to use when registering the schema.
                               Available: TopicName, RecordName, TopicRecordName
                               [default: TopicRecordName]
  --value-strategy=<strategy>  Value subject name strategy to use when registering the schema.
                               Available: TopicName, RecordName, TopicRecordName
                               [default: TopicRecordName]
  --topic=<topic>              Topic name to be used when using TopicRecordName strategy.
  --test                       Test schema compatibility before registration
                               [default: True].
"""
import os

from docopt import docopt

from confluent_kafka_helpers.schema_registry.bin import register, test
from confluent_kafka_helpers.schema_registry.client import schema_registry

args = docopt(__doc__)
command = args.get('<command>')
subject = args.get('--subject')
schema_file = args.get('--file')
url = os.getenv('SCHEMA_REGISTRY_URL', args.get('--url'))
automatic = args.get('--automatic')
folder = args.get('--folder')
key_strategy = args.get('--key-strategy')
value_strategy = args.get('--value-strategy')
topic = args.get('--topic')
test_compatibility = args.get('--test')


def main():
    schema_registry.init(url=url, key_strategy=key_strategy, value_strategy=value_strategy)
    if command == 'register':
        register.run(
            automatic=automatic, test_compatibility=test_compatibility, topic=topic, folder=folder,
            subject=subject, schema_file=schema_file
        )
    elif command == 'test':
        test.run(
            automatic=automatic, folder=folder, schema_file=schema_file, subject=subject,
            topic=topic
        )


if __name__ == '__main__':
    main()
