# Confluent Kafka helpers

[![build](https://circleci.com/gh/fyndiq/confluent_kafka_helpers/tree/master.svg?style=shield)](https://circleci.com/gh/fyndiq/confluent_kafka_helpers/tree/master)
[![version](https://img.shields.io/pypi/v/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)
[![downloads](https://img.shields.io/pypi/dm/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)
[![license](https://img.shields.io/pypi/l/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)

Library built on top of [Confluent Kafka
Python](https://github.com/confluentinc/confluent-kafka-python) adding
abstractions for consuming and producing messages in a more Pythonic way.

## Graceful shutdown

Importing `confluent_kafka_helpers` installs handlers for `SIGTERM` and `SIGINT`
that **do not exit the process immediately**. Instead the in-flight message handler always runs to completion before the loop exits.

### Chaining with other signal handlers

If another library (ddtrace, OpenTelemetry SDK, gunicorn, uvicorn, ...) has
already installed a SIGTERM/SIGINT handler before `confluent_kafka_helpers`
is imported, the existing handler is captured and **still called** after the
flag is set.

### Polling the flag from your own code

Long-running handlers can check the flag and bail out early between subtasks:

```python
from confluent_kafka_helpers import is_shutdown_requested

for message in consumer:
    do_step_one(message)
    if is_shutdown_requested():
        break
    do_step_two(message)
```

### Operational notes

- `SIGKILL` cannot be intercepted. Kubernetes will still send it after
  `terminationGracePeriodSeconds` if the process hasn't exited. The default value for this in k8s is 30 seconds. If handling a single message can take longer than that the application need to configure a longer `terminationGracePeriodSeconds` in the k8s manifest
- The flag is process-wide. Tests that exercise it must clear it (see
  `tests/test_signals.py` for the autouse reset fixture pattern).

## Schema registry retries

`AvroConsumer`, `AvroLazyConsumer`, `AvroProducer` and `AvroSchemaRegistry` talk to the
schema registry through `confluent_kafka_helpers.schema_registry.SchemaRegistryClient`, a
subclass of confluent's legacy `CachedSchemaRegistryClient`.

A registry response with a 5xx status or a non-JSON body (an empty body, a proxy error page) is
retried with exponential backoff: 0.5s doubling, capped at 10s per sleep. Other responses are
returned as before, so a 404 still means "not found".

| Client | Default budget |
|---|---|
| `AvroConsumer`, `AvroLazyConsumer` | 120s |
| `AvroProducer`, `AvroSchemaRegistry` | 10s |

Consumers block on the schema lookup anyway, so they wait long enough to ride out a registry
restart without crashing the process. Producers can sit on a request path, so they give up
quickly. Override the budget per client with `schema.registry.retry.timeout` (seconds) in the
consumer or producer config.

When the budget runs out the client raises `ClientError` with the registry's status and body,
for example `Schema registry GET /schemas/ids/399 failed: HTTP 502: '<html>Bad Gateway</html>'`.
During decoding this surfaces as `SerializerError: unable to fetch schema with id 399: ...`.

The upstream client has a fallback that reads `response.content` from a urllib3 response,
which has no such attribute, so one non-JSON response used to raise `AttributeError`. This
client replaces that fallback.

## OpenTelemetry (OTEL)

### Test generation of spans

Make sure you have `opentelemetry-sdk` installed.

Add this code to your applications entry point as early as possible, e.g:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

...
consumer = init_app()
consumer.consume()
```

All spans will now be printed to the std out instead of getting exported.

**NOTE!** This will most probably conflict if you use another SDK, e.g: `ddtrace` with OTEL enabled
(`DD_TRACE_OTEL_ENABLED=true`), since it will set up it's own read-only tracer provider. This is mainly for debugging
the generated spans from an OTEL point of view. Once verified, you should also test with your preferred SDK/exporter.
