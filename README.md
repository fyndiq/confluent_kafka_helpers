# Confluent Kafka helpers

[![CI](https://github.com/fyndiq/confluent_kafka_helpers/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/fyndiq/confluent_kafka_helpers/actions/workflows/ci.yml)
[![version](https://img.shields.io/pypi/v/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)
[![downloads](https://img.shields.io/pypi/dm/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)
[![license](https://img.shields.io/pypi/l/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)

Library built on top of [Confluent Kafka
Python](https://github.com/confluentinc/confluent-kafka-python) adding
abstractions for consuming and producing messages in a more Pythonic way.

## Development

Install [uv](https://docs.astral.sh/uv/) 0.13.1 and [just](https://just.systems/)
and run `just setup` to install the locked development dependencies with
Python 3.10. Run `just test` for unit tests and lint checks, and `just build`
to build a wheel and source distribution in `dist/`. `just lock` updates the
lockfile after dependency changes.

Runtime requirements and package metadata are defined in `pyproject.toml`.
`uv.lock` fixes development versions. Commitizen 4.13.9 uses the uv version
provider and annotated `v<version>` tags. Publishing runs through the release
workflow. See [Releases](docs/releases.md) for setup, prereleases, and recovery.

## Kafka errors and commits

- Nonfatal global `_TRANSPORT`, `_ALL_BROKERS_DOWN`, `_RESOLVE` and `_TIMED_OUT`
  errors log warnings. Metrics and custom error callbacks still run. Native
  librdkafka log severity is unchanged.
- Fatal and other global errors still raise `KafkaError`; delivery failures still
  raise `KafkaDeliveryError`.
- Synchronous commits retry transient coordinator errors, request timeouts and
  broker unavailability up to three attempts, with 0.1 and 0.2 second delays.
  Other failures raise, except for lost membership or assignment.
- Lost membership or assignment emits a warning and commit error metric without
  retrying stale offsets. Handlers must tolerate redelivery. `commit()` returns
  `None`, which does not confirm success.
- Asynchronous failures emit metrics and log warnings for transient errors or
  assignment loss, and errors otherwise. Custom `on_commit` callbacks own
  reporting. Asynchronous commit failures are not retried.

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

## Header propagation

When a service consumes one Kafka message and produces another, this library can copy selected
headers from the old message to the new one. These headers are called propagated headers.

The library always tries to copy these headers:

- `origin_action_type`: the action that started the chain of messages.
- `action_started_at`: when that action started, as epoch milliseconds.

They help downstream services measure how long the whole chain took. Services do not need to list
these two headers in `headers.propagate`; the library includes them by default. If an incoming
message does not have one of these headers, the library leaves it out and does not create it.

Services can still add more headers with `headers.propagate`, for example:

```python
AvroConsumer({**config, "headers.propagate": ["correlation_id"]})
```

## Batch consumption

`AvroConsumer.batches()` yields lists of messages, e.g. for bulk writes:

```python
consumer = AvroConsumer({
    ...,
    "batch_max_size": 100,  # default 100
    "batch_max_wait": 1.0,  # seconds, default 1.0
})

def handle_batch(batch):
    bulk_write([message.value for message in batch])
```

Usually you pass the handler to eventsourcing_helpers (`consume_batches(handle_batch)`), which
iterates `consumer.batches()` and commits the offsets after the handler returns.

- Each `consume()` read returns up to `batch_max_size` messages. `batch_max_wait` bounds
  the wait from the start of the read, including when idle. With `non_blocking`, reads are
  also capped at `poll_timeout` and empty reads yield `[]`. Blocking mode skips empty reads.
- On shutdown (or EOF with `stop_on_eof`) the partial batch is yielded, then iteration stops.
- If you iterate `batches()` yourself, commit after each batch: `commit()` without arguments
  commits everything polled so far, i.e. the whole batch.
- Don't mix `batches()` and per-message iteration on the same consumer.
- **Handlers must be idempotent.** Delivery is at-least-once per batch: if the process crashes,
  the handler raises, the commit fails or a rebalance happens before the commit, the whole batch
  is delivered again, including messages whose db writes were already committed.

### Propagated headers

The consumer config `headers.propagate` lists header keys (e.g. `correlation_id`) that
`producer.produce()` forwards on the messages it produces. When consuming one message at a time,
this is automatic while the message is handled.

When consuming in batches, no headers are set automatically, because there is no single current
message. Each message has `message.propagated_headers` (the default and configured keys only). Use
`message.propagate()` around the produce calls that come from that message:

```python
def handle_batch(batch):
    bulk_write([message.value for message in batch])  # persist first, commit the db
    for message in batch:
        with message.propagate():
            producer.produce(build_event(message))
    producer.flush()  # recommended, see below
```

`produce()` only queues the message in the producer's buffer, and the batch's offsets are
committed right after the handler returns. We recommend flushing the producer before that, e.g.
`producer.flush()` at the end of the handler. With eventsourcing_helpers you can instead set
`"flush": True` in the producer config, which flushes after every `produce()`. Without a flush, a
crash after the commit (e.g. `SIGKILL` or OOM, which skip the flush on exit) loses the queued
messages, and they are not produced again because the input messages count as consumed. The
same applies when consuming one message at a time.

Outside a `propagate()` block, nothing is forwarded. If you produce from data that you collected
during the batch, keep a reference to the source message with that data. Explicit `headers=`
passed to `produce()` override propagated headers with the same key.

### Tracing

Each batch gets one `kafka.consume` span. It starts a new trace and links to every message's
trace context.

OpenTelemetry keeps at most 128 links per span by default and silently drops the rest. If you raise
`batch_max_size` above 128, also raise `OTEL_SPAN_LINK_COUNT_LIMIT`.

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
