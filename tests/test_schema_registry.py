import io
import json
import struct
from unittest.mock import MagicMock, patch

import pytest
import requests
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.error import ClientError
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from urllib3 import HTTPResponse

from confluent_kafka_helpers import schema_registry

from tests import config


class CachedSchemaRegistryClientMock(MagicMock):
    get_latest_schema = MagicMock()
    get_latest_schema.return_value = ["a", "b", "c"]
    register = MagicMock()


mock_client = CachedSchemaRegistryClientMock()
mock_serializer = MagicMock()


@pytest.fixture(scope="module")
def avro_schema_registry():
    url = config.Config.KAFKA_CONSUMER_CONFIG["schema.registry.url"]
    return schema_registry.AvroSchemaRegistry(url, mock_client, mock_serializer)


def test_init(avro_schema_registry):
    mock_client.assert_called_once_with(
        url=config.Config.KAFKA_CONSUMER_CONFIG["schema.registry.url"]
    )


def test_get_latest_schema(avro_schema_registry):
    subject = "a"
    avro_schema_registry.get_latest_schema(subject)
    mock_client.get_latest_schema.assert_called_once_with(subject)


@patch("confluent_kafka_helpers.schema_registry.avro.load", MagicMock())
def test_register_schema(avro_schema_registry):
    avro_schema_registry.register_schema("a", "b")
    assert mock_client.register.call_count == 1


REGISTRY_URL = "https://user:pass@registry.example.com"
SCHEMA_ID = 133
SCHEMA_BODY = {"schema": '"string"'}


def https_response(body: bytes, status: int) -> HTTPResponse:
    # A file-like body reads like a socket, so an empty body is b'' rather than None.
    return HTTPResponse(body=io.BytesIO(body), status=status, preload_content=True)


def https_json_response(data: dict, status: int = 200) -> HTTPResponse:
    return https_response(json.dumps(data).encode(), status)


def http_response(body: bytes, status: int) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = body
    return response


@pytest.fixture
def sleeps(mocker):
    """Fake clock: `sleep` advances `monotonic`. Returns the list of slept durations."""
    now = 0.0
    sleeps = []

    def monotonic():
        return now

    def sleep(seconds):
        nonlocal now
        now += seconds
        sleeps.append(seconds)

    mocker.patch("confluent_kafka_helpers.schema_registry.time.monotonic", side_effect=monotonic)
    mocker.patch("confluent_kafka_helpers.schema_registry.time.sleep", side_effect=sleep)
    return sleeps


@pytest.fixture
def logger(mocker):
    return mocker.patch("confluent_kafka_helpers.schema_registry.logger")


@pytest.fixture
def client():
    return schema_registry.SchemaRegistryClient({"url": REGISTRY_URL}, retry_timeout=120)


@pytest.fixture
def https_request(mocker, client):
    return mocker.patch.object(client._https_session, "request")


class TestSchemaRegistryClient:
    def test_upstream_client_crashes_on_non_json_https_response(self, mocker):
        upstream = CachedSchemaRegistryClient({"url": REGISTRY_URL})
        mocker.patch.object(
            upstream._https_session, "request", return_value=https_response(b"", 502)
        )

        with pytest.raises(
            AttributeError, match="'HTTPResponse' object has no attribute 'content'"
        ):
            upstream.get_by_id(SCHEMA_ID)

    def test_get_by_id_retries_non_json_response_until_registry_recovers(
        self, client, https_request, sleeps, logger
    ):
        https_request.side_effect = [https_response(b"", 502), https_json_response(SCHEMA_BODY)]

        schema = client.get_by_id(SCHEMA_ID)

        assert str(schema) == '"string"'
        assert https_request.call_count == 2
        assert sleeps == [0.5]
        logger.warning.assert_called_once_with(
            "Schema registry request failed, retrying",
            method="GET",
            path=f"/schemas/ids/{SCHEMA_ID}",
            status=502,
            body="''",
            retry_in=0.5,
        )

    def test_get_by_id_raises_client_error_with_status_and_body_when_non_json_persists(
        self, client, https_request, sleeps
    ):
        https_request.side_effect = lambda *a, **kw: https_response(b"<html>gateway</html>", 200)

        with pytest.raises(ClientError) as exc_info:
            client.get_by_id(SCHEMA_ID)

        assert exc_info.value.http_code == 200
        assert str(exc_info.value) == (
            f"Schema registry GET /schemas/ids/{SCHEMA_ID} failed: HTTP 200: '<html>gateway</html>'"
        )
        assert sum(sleeps) <= 120
        assert sleeps[:6] == [0.5, 1, 2, 4, 8, 10]
        assert https_request.call_count == len(sleeps) + 1

    def test_get_by_id_raises_client_error_when_server_error_persists(
        self, client, https_request, sleeps
    ):
        https_request.side_effect = lambda *a, **kw: https_json_response(
            {"error_code": 50301, "message": "unavailable"}, 503
        )

        with pytest.raises(ClientError, match="HTTP 503: .*50301.*unavailable") as exc_info:
            client.get_by_id(SCHEMA_ID)

        assert exc_info.value.http_code == 503
        assert https_request.call_count > 1

    def test_get_by_id_does_not_retry_not_found(self, client, https_request, sleeps):
        https_request.return_value = https_json_response(
            {"error_code": 40403, "message": "Schema not found"}, 404
        )

        assert client.get_by_id(SCHEMA_ID) is None
        assert https_request.call_count == 1
        assert sleeps == []

    def test_send_request_returns_client_errors_without_retrying(
        self, client, https_request, sleeps
    ):
        https_request.return_value = https_json_response({"message": "Unauthorized"}, 401)

        result = client._send_request(f"{client.url}/subjects")

        assert result == ({"message": "Unauthorized"}, 401)
        assert https_request.call_count == 1
        assert sleeps == []

    def test_send_request_retries_non_json_response_over_plain_http(self, mocker, sleeps):
        client = schema_registry.SchemaRegistryClient({"url": "http://registry.example.com"})
        session_request = mocker.patch.object(
            client._session,
            "request",
            side_effect=[
                http_response(b"", 502),
                http_response(json.dumps(SCHEMA_BODY).encode(), 200),
            ],
        )

        assert str(client.get_by_id(SCHEMA_ID)) == '"string"'
        assert session_request.call_count == 2
        assert sleeps == [0.5]

    def test_decode_message_reports_registry_status_and_body(self, client, https_request, sleeps):
        https_request.side_effect = lambda *a, **kw: https_response(b"Bad Gateway", 502)
        message = struct.pack(">bI", 0, SCHEMA_ID) + b"\x00"

        with pytest.raises(
            SerializerError,
            match=f"unable to fetch schema with id {SCHEMA_ID}: .*HTTP 502: 'Bad Gateway'",
        ):
            MessageSerializer(client).decode_message(message)

    def test_retry_timeout_defaults_to_producer_budget(self):
        client = schema_registry.SchemaRegistryClient({"url": REGISTRY_URL})

        assert client.retry_timeout == schema_registry.DEFAULT_RETRY_TIMEOUT == 10

    def test_retry_timeout_config_key_overrides_argument(self):
        conf = {"url": REGISTRY_URL, "retry.timeout": "30"}

        client = schema_registry.SchemaRegistryClient(conf, retry_timeout=120)

        assert client.retry_timeout == 30
        assert conf == {"url": REGISTRY_URL, "retry.timeout": "30"}

    def test_accepts_plain_url(self):
        client = schema_registry.SchemaRegistryClient(REGISTRY_URL)

        assert client.url == "https://registry.example.com"

    def test_avro_schema_registry_uses_retrying_client_by_default(self):
        registry = schema_registry.AvroSchemaRegistry(REGISTRY_URL)

        assert isinstance(registry.client, schema_registry.SchemaRegistryClient)


class TestSplitSchemaRegistryConfig:
    def test_moves_schema_registry_settings_to_registry_config(self):
        registry_config, client_config = schema_registry.split_schema_registry_config(
            {
                "bootstrap.servers": "kafka:9092",
                "group.id": "group",
                "schema.registry.url": REGISTRY_URL,
                "schema.registry.retry.timeout": 30,
            }
        )

        assert registry_config == {"url": REGISTRY_URL, "retry.timeout": 30}
        assert client_config == {"bootstrap.servers": "kafka:9092", "group.id": "group"}

    def test_sasl_inherit_copies_sasl_credentials(self):
        registry_config, client_config = schema_registry.split_schema_registry_config(
            {
                "bootstrap.servers": "kafka:9092",
                "sasl.mechanisms": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
                "schema.registry.url": "https://registry.example.com",
                "schema.registry.basic.auth.credentials.source": "SASL_INHERIT",
            }
        )

        assert registry_config == {
            "url": "https://registry.example.com",
            "basic.auth.credentials.source": "SASL_INHERIT",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": "user",
            "sasl.password": "pass",
        }
        assert client_config["sasl.username"] == "user"
