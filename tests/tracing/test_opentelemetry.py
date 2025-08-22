from unittest.mock import ANY, Mock, call, patch

import pytest
from opentelemetry.context.context import Context
from opentelemetry.trace import Link, Span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from confluent_kafka_helpers.tracing import OpenTelemetryBackend


@pytest.fixture
def mock_tracer():
    return Mock()


@pytest.fixture
def backend(mock_tracer):
    return OpenTelemetryBackend(name="test_service", tracer=mock_tracer)


def test_start_span(backend, mock_tracer):
    span_mock = Mock(spec=Span)
    mock_tracer.return_value.start_span.return_value = span_mock

    with backend.start_span("test_operation") as span:
        expected_calls = [
            call.set_attribute("messaging.system", "kafka"),
            call.set_attribute("span.type", "custom"),
            call.set_attribute("operation.name", "test_operation"),
            call.set_attribute("service.name", "kafka"),
        ]
        span.assert_has_calls(expected_calls)

    span_mock.end.assert_called_once()


def test_inject_headers(backend):
    headers = {"foo": "bar"}
    with patch.object(TraceContextTextMapPropagator, "inject", autospec=True) as inject_mock:
        backend.inject_headers(headers)
        inject_mock.assert_called_once_with(ANY, {"foo": "bar"})


def test_extract_headers(backend):
    headers = {"foo": "bar"}
    with patch.object(TraceContextTextMapPropagator, "extract", autospec=True) as extract_mock:
        mock_context = Mock(spec=Context)
        extract_mock.return_value = mock_context
        context = backend.extract_headers(headers)
        extract_mock.assert_called_once_with(ANY, {"foo": "bar"})
        assert context is mock_context


def test_extract_links(backend):
    mock_context = Mock(spec=Context)
    mock_span_context = Mock()
    mock_context.values.return_value = [Mock(get_span_context=Mock(return_value=mock_span_context))]

    links = backend.extract_links(mock_context)
    assert len(links) == 1
    assert isinstance(links[0], Link)
    assert links[0].context == mock_span_context


def test_extract_links_without_span_context(backend):
    mock_context = Mock(spec=Context)
    mock_context.values.return_value = [Mock(spec=[])]

    links = backend.extract_links(mock_context)
    assert len(links) == 0
