import sys

import requests
import structlog
from confluent_kafka import avro
from fastavro.schema import load_schema

from confluent_kafka_helpers.schema_registry.bin import utils
from confluent_kafka_helpers.schema_registry.client import schema_registry

logger = structlog.get_logger(__name__)


class Register:
    def __init__(
        self, test_compatibility: bool, topic: str, schema_registry=schema_registry,
        schema_validator=load_schema
    ):
        self.schema_registry = schema_registry
        self.schema_validator = schema_validator
        self.test_compatibility = test_compatibility
        self.topic = topic

    def _verify_schema(self, schema_file, subject, is_key):
        logger.info("Verifying schema file", schema_file=schema_file)
        try:
            self.schema_validator(schema_file)
        except Exception as e:
            logger.error("Invalid schema", schema_file=schema_file, error=e)
            sys.exit(1)

    def _test_compatibility(self, schema_file, subject, is_key):
        if self.test_compatibility:
            compatible = self.schema_registry.test_compatibility(
                schema=avro.load(schema_file), subject=subject, topic=self.topic, is_key=is_key
            )
            if not compatible:
                logger.error("Schema not compatible with latest version")
                sys.exit(1)
            logger.info("Schema is compatible with latest version")

    def _register_schema(self, schema_file, subject, is_key):
        try:
            schema_id = self.schema_registry.register_schema(
                schema=avro.load(schema_file), subject=subject, topic=self.topic, is_key=is_key
            )
            logger.info("Registered schema with id", schema_id=schema_id)
        except requests.exceptions.ConnectionError:
            logger.error("Could not connect to schema registry")
            sys.exit(1)

    def _register(self, schema_file, subject=None, is_key=False):
        self._verify_schema(schema_file=schema_file, subject=subject, is_key=is_key)
        self._test_compatibility(schema_file=schema_file, subject=subject, is_key=is_key)
        self._register_schema(schema_file=schema_file, subject=subject, is_key=is_key)


class ManualRegister(Register):
    def register(self, subject: str, schema_file: str, *args: tuple, **kwargs: dict):
        self._register(subject=subject, schema_file=schema_file)


class AutomaticRegister(Register):
    def _register_schemas(self, schema_files):
        for schema_file, is_key in schema_files:
            self._register(schema_file=schema_file, is_key=is_key)

    def register(self, folder: str, *args: tuple, **kwargs: dict):
        self._register_schemas(utils.get_schema_files(folder=folder))


def run(
    automatic: bool, test_compatibility: bool, topic: str, folder: str, subject: str,
    schema_file: str
):
    register_cls = AutomaticRegister if automatic else ManualRegister
    register = register_cls(test_compatibility=test_compatibility, topic=topic)
    register.register(folder=folder, subject=subject, schema_file=schema_file)
