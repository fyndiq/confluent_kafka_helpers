import os
import sys

import structlog
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

        self._client = client(self._hostname)
        self._schema_validator = schema_validator
        self._resolver = resolver

    def _verify_schema(self, schema_file, subject):
        logger.info("Verifying schema", file=schema_file)
        try:
            self._schema_validator(schema_file)
        except Exception as e:
            logger.error("Invalid schema", file=schema_file, error=e)
            sys.exit(1)
        if self._test_compatibility:
            compatible = self._client.test_compatibility(subject=subject, schema_file=schema_file)
            if not compatible:
                sys.exit(1)
        return schema_file

    def _register_schema(self, schema_file, subject):
        try:
            self._client.register_schema(subject, schema_file)
        except ConnectionError:
            logger.error("Could not connect to schema registry", url=SCHEMA_REGISTRY_URL)
            sys.exit(1)

    def _register(self, subject, schema_file):
        self._register_schema(self._verify_schema(schema_file, subject), subject)

    @staticmethod
    def factory(automatic):
        registrator = ManualRegistrator
        if automatic:
            registrator = AutomaticRegistrator
        return registrator


class ManualRegistrator(SchemaRegistrator):
    def register(self, subject, schema_file):
        self._register(subject, schema_file)


class AutomaticRegistrator(SchemaRegistrator):
    def _get_schema_files_and_subjects(self):
        schema_files = utils.get_schema_files(folder=self._schemas_folder)
        if not schema_files:
            logger.error("Could not find any schemas in folder", folder=self._schemas_folder)
            sys.exit(1)
        for schema_file, is_key in schema_files:
            resolver = self._resolver.factory(
                strategy=self._key_strategy if is_key else self._value_strategy
            )
            subject = resolver(is_key=is_key).get_subject(schema_file=schema_file)
            yield schema_file, subject

    def _register_schemas(self, schemas):
        for schema_file, subject in schemas:
            self._register(subject, schema_file)

    def register(self, *args, **kwargs):
        self._register_schemas(self._get_schema_files_and_subjects())
