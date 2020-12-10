from unittest.mock import Mock

import opentracing
import pytest

from confluent_kafka_helpers.tracing.backends.opentracer import (
    OpenTracerBackend
)


@pytest.fixture
def opentracer():
    return OpenTracerBackend(tracer=Mock())


def test_start_active_span_pass_correct_kwargs_and_finish_span(opentracer):
    with opentracer.start_active_span(operation_name='foo'):
        expected_kwargs = {
            'operation_name': 'foo',
            'tags': {
                'component': 'confluent_kafka_helpers',
                'peer.service': 'kafka',
            }
        }
        opentracer._tracer.start_active_span.assert_called_once_with(
            **expected_kwargs
        )
    span = opentracer._tracer.start_active_span.return_value.span
    assert span.finish.call_count == 1


def test_start_active_span_logs_exception_and_finish_span(opentracer):
    with pytest.raises(ZeroDivisionError):
        with opentracer.start_active_span(operation_name='foo'):
            raise ZeroDivisionError('bar')

    expected_log_args = {
        'error.kind': 'ZeroDivisionError',
        'error.object': 'ZeroDivisionError',
        'event': 'error',
        'message': 'bar'
    }
    span = opentracer._tracer.start_active_span.return_value.span
    log_args = span.log_kv.call_args.args[0]
    log_args.pop('stack')

    span.set_tag.assert_called_once_with('error', True)
    assert log_args == expected_log_args
    assert span.finish.call_count == 1


def test_start_span_pass_correct_kwargs_and_finish_span(opentracer):
    with opentracer.start_span(operation_name='foo'):
        expected_kwargs = {
            'operation_name': 'foo',
            'tags': {
                'component': 'confluent_kafka_helpers',
                'peer.service': 'kafka',
            }
        }
        opentracer._tracer.start_span.assert_called_once_with(**expected_kwargs)
    assert opentracer._tracer.start_span.return_value.finish.call_count == 1


def test_start_span_logs_exception_and_finish_span(opentracer):
    with pytest.raises(ZeroDivisionError):
        with opentracer.start_span(operation_name='foo'):
            raise ZeroDivisionError('bar')

    expected_log_args = {
        'error.kind': 'ZeroDivisionError',
        'error.object': 'ZeroDivisionError',
        'event': 'error',
        'message': 'bar'
    }
    span = opentracer._tracer.start_span.return_value
    log_args = span.log_kv.call_args.args[0]
    log_args.pop('stack')

    span.set_tag.assert_called_once_with('error', True)
    assert log_args == expected_log_args
    assert span.finish.call_count == 1


def test_extract_headers_and_start_span_with_parent_ctx_pass_correct_kwargs(
    opentracer
):
    headers = {
        'x-datadog-parent-id': '1',
        'x-datadog-sampling-priority': '1',
        'x-datadog-trace-id': '2'
    }
    with opentracer.extract_headers_and_start_span(
        operation_name='foo', headers=headers
    ):
        expected_extract_kwargs = {
            'format': 'text_map',
            'carrier': {
                'x-datadog-parent-id': '1',
                'x-datadog-sampling-priority': '1',
                'x-datadog-trace-id': '2'
            },
        }
        opentracer._tracer.extract.assert_called_once_with(
            **expected_extract_kwargs
        )
        parent_context = opentracer._tracer.extract.return_value
        expected_start_span_kwargs = {
            'operation_name': 'foo',
            'references': [opentracing.follows_from(parent_context)],
            'tags': {
                'component': 'confluent_kafka_helpers',
                'peer.service': 'kafka',
            }
        }
        opentracer._tracer.start_active_span.assert_called_once_with(
            **expected_start_span_kwargs
        )


@pytest.mark.parametrize(
    'exception', [
        opentracing.InvalidCarrierException,
        opentracing.SpanContextCorruptedException
    ]
)
def test_extract_headers_and_start_span_without_parent_ctx_pass_correct_kwargs(
    opentracer, exception
):
    opentracer._tracer.extract.side_effect = exception
    with opentracer.extract_headers_and_start_span(
        operation_name='foo', headers={}
    ):
        expected_start_span_kwargs = {
            'operation_name': 'foo',
            'tags': {
                'component': 'confluent_kafka_helpers',
                'peer.service': 'kafka',
            }
        }
        opentracer._tracer.start_active_span.assert_called_once_with(
            **expected_start_span_kwargs
        )


def test_inject_headers_and_start_span_pass_correct_kwargs(opentracer):
    with opentracer.inject_headers_and_start_span(
        operation_name='foo', headers={'foo': 'bar'}
    ):
        expected_start_span_kwargs = {
            'operation_name': 'foo',
            'tags': {
                'component': 'confluent_kafka_helpers',
                'peer.service': 'kafka',
            }
        }
        opentracer._tracer.start_span.assert_called_once_with(
            **expected_start_span_kwargs
        )
        active_span = opentracer._tracer.active_span
        expected_inject_kwargs = {
            'span_context': active_span.context,
            'carrier': {
                'foo': 'bar'
            },
            'format': 'text_map',
        }
        opentracer._tracer.inject.assert_called_once_with(
            **expected_inject_kwargs
        )
