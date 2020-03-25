import os
import sys

import structlog
from confluent_kafka import avro
from fastavro.schema import load_schema
from requests.exceptions import ConnectionError

from confluent_kafka_helpers.schema_registry import utils
from confluent_kafka_helpers.schema_registry.client import SchemaRegistryClient
from confluent_kafka_helpers.schema_registry.subject import SubjectNameResolver

logger = structlog.get_logger(__name__)
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')


class SchemaRegistrator:
    def __init__(
        self, client=SchemaRegistryClient, schema_validator=load_schema,
        resolver=SubjectNameResolver, **kwargs
    ):
        self._hostname = os.getenv('SCHEMA_REGISTRY_URL', kwargs['--hostname'])
        self._schemas_folder = kwargs['--folder']
        self._key_strategy = kwargs['--key-strategy']
        self._value_strategy = kwargs['--value-strategy']
        self._test_compatibility = kwargs['--test']

        self._client = client(
            url=self._hostname, key_strategy=self._key_strategy, value_strategy=self._value_strategy
        )
        self._schema_validator = schema_validator
        self._resolver = resolver

    def _verify_schema(self, schema_file, subject, is_key):
        logger.info("Verifying schema file", schema_file=schema_file)
        try:
            self._schema_validator(schema_file)
        except Exception as e:
            logger.error("Invalid schema", schema_file=schema_file, error=e)
            sys.exit(1)
        if self._test_compatibility:
            compatible = self._client.test_compatibility(
                schema_file=schema_file, subject=subject, is_key=is_key
            )
            if not compatible:
                sys.exit(1)

    def _register_schema(self, schema_file, subject, is_key):
        try:
            self._client.register_schema(schema_file=schema_file, subject=subject, is_key=is_key)
        except ConnectionError:
            logger.error("Could not connect to schema registry", url=SCHEMA_REGISTRY_URL)
            sys.exit(1)

    def _register(self, schema_file, subject=None, is_key=False):
        self._verify_schema(schema_file=schema_file, subject=subject, is_key=is_key)
        self._register_schema(schema_file=schema_file, subject=subject, is_key=is_key)

    @staticmethod
    def factory(automatic):
        registrator = ManualRegistrator
        if automatic:
            registrator = AutomaticRegistrator
        return registrator


class ManualRegistrator(SchemaRegistrator):
    def register(self, subject, schema_file):
        self._register(subject=subject, schema_file=schema_file)


class AutomaticRegistrator(SchemaRegistrator):
    def _register_schemas(self, schema_files):
        for schema_file, is_key in schema_files:
            self._register(schema_file=schema_file, is_key=is_key)

    def register(self, *args, **kwargs):
        self._register_schemas(utils.get_schema_files(folder=self._schemas_folder))