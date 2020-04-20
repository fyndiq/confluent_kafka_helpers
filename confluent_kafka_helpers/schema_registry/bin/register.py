import sys

import requests
import structlog
from confluent_kafka import avro
from fastavro.schema import load_schema

from confluent_kafka_helpers.schema_registry.bin import test, utils
from confluent_kafka_helpers.schema_registry.client import schema_registry

logger = structlog.get_logger(__name__)


def _validate_schema(schema_file: str, subject: str, is_key: bool, validator=load_schema) -> None:
    logger.info("Validating schema file", schema_file=schema_file)
    try:
        validator(schema_file)
    except Exception as e:
        logger.error("Invalid schema", schema_file=schema_file, error=e)
        sys.exit(1)


def _register_schema(schema_file: str, subject: str, topic: str, is_key: bool) -> None:
    try:
        schema_id = schema_registry.register_schema(
            schema=avro.load(schema_file), subject=subject, topic=topic, is_key=is_key
        )
        logger.info("Registered schema with id", schema_id=schema_id)
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to schema registry")
        sys.exit(1)


def register(
    schema_file: str, test_compatibility: bool, subject: str, topic: str, is_key: bool = False
) -> None:
    _validate_schema(schema_file=schema_file, subject=subject, is_key=is_key)
    if test_compatibility:
        test.test_compatibility(schema_file=schema_file, subject=subject, is_key=is_key)
    _register_schema(schema_file=schema_file, subject=subject, topic=topic, is_key=is_key)


def run(
    automatic: bool, test_compatibility: bool, topic: str, folder: str, subject: str,
    schema_file: str
) -> None:
    if automatic:
        for schema_file, is_key in utils.get_schema_files(folder=folder):
            register(schema_file=schema_file, is_key=is_key, test_compatibility=test_compatibility)
    register(
        subject=subject, schema_file=schema_file, topic=topic, test_compatibility=test_compatibility
    )
