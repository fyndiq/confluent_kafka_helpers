from unittest.mock import MagicMock, patch

import pytest
import urllib3
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.error import ClientError

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


STALE_CONNECTION_ERROR = urllib3.exceptions.ProtocolError(
    "Connection aborted.", ConnectionResetError("Remote end closed connection without response")
)


@pytest.fixture
def retrying_client():
    with patch("confluent_kafka_helpers.schema_registry.time.sleep") as sleep:
        client = schema_registry.RetryingSchemaRegistryClient({"url": "https://registry:443"})
        client._sleep = sleep
        yield client


@pytest.mark.parametrize("method", ["register", "check_registration"])
def test_retrying_client_succeeds_on_second_attempt(retrying_client, method):
    with patch.object(
        CachedSchemaRegistryClient, method, side_effect=[STALE_CONNECTION_ERROR, 42]
    ) as parent:
        assert getattr(retrying_client, method)("topic-key", "schema") == 42
    assert parent.call_count == 2
    retrying_client._sleep.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        STALE_CONNECTION_ERROR,
        urllib3.exceptions.NewConnectionError(None, "refused"),
        urllib3.exceptions.MaxRetryError(None, "/subjects", "too many"),
        ConnectionResetError("reset"),
    ],
)
def test_retrying_client_gives_up_after_max_attempts(retrying_client, error):
    with patch.object(CachedSchemaRegistryClient, "register", side_effect=error) as parent:
        with pytest.raises(type(error)):
            retrying_client.register("topic-key", "schema")
    assert parent.call_count == 3
    assert retrying_client._sleep.call_count == 2


def test_retrying_client_does_not_retry_non_transient_errors(retrying_client):
    error = ClientError("Incompatible Avro schema:409")
    with patch.object(CachedSchemaRegistryClient, "register", side_effect=error) as parent:
        with pytest.raises(ClientError):
            retrying_client.register("topic-key", "schema")
    assert parent.call_count == 1
    retrying_client._sleep.assert_not_called()


def test_retrying_client_retries_real_register_call(retrying_client):
    retrying_client._send_request = MagicMock(
        side_effect=[STALE_CONNECTION_ERROR, ({"id": 7}, 200)]
    )
    assert retrying_client.register("topic-key", "schema") == 7
    assert retrying_client._send_request.call_count == 2
    # cached afterwards, no further requests
    assert retrying_client.register("topic-key", "schema") == 7
    assert retrying_client._send_request.call_count == 2
