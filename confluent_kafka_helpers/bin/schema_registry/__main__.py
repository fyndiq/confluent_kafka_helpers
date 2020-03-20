#!/usr/bin/env python3
"""
Schema registry client.

Usage:
  schema-registry register (-s <subject> -f <file> | --automatic [--folder=<folder>]
                           [--key-strategy=<key-strategy>] [--value-strategy=<value-strategy>])
                           [-h <hostname>] [--test]

Commands:
    register                        Register a schema to schema registry.

Options:
  -h --hostname=<hostname>          Schema registry hostname [default: http://localhost:8081].
  -s --subject=<subject>            Subject the schema will be registered to.
  -f --file=<file>                  Avro schema file (*.avsc).
  --automatic                       Automatic find and register schemas in given folder
                                    [default: False].
  --key-strategy=<key-strategy>     Key subject name strategy to use when registering the schema.
                                    Available: TopicName, RecordName, TopicRecordName
                                    [default: TopicRecordName]
  --value-strategy=<value-strategy> Value subject name strategy to use when registering the schema.
                                    Available: TopicName, RecordName, TopicRecordName
                                    [default: TopicRecordName]
  --folder=<folder>                 Folder with Avro schemas [default: schemas].
  --test                            Test schema compatibility before registration.
                                    [default: False].
"""
from docopt import docopt

from confluent_kafka_helpers.bin.schema_registry.registrators import SchemaRegistrator

args = docopt(__doc__)
register = args.pop('register')
subject = args.pop('--subject')
schema = args.pop('--file')
automatic = args.pop('--automatic')


def main():
    if register:
        registrator = SchemaRegistrator.factory(automatic)(**args)
        registrator.register(subject, schema)


if __name__ == '__main__':
    main()
