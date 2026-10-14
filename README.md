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

- A batch is yielded when it has `batch_max_size` messages, or `batch_max_wait` seconds after
  its first message, whichever comes first. An idle consumer yields nothing
  (with `non_blocking`, it yields `[]` every `batch_max_wait`).
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
message. Each message has `message.propagated_headers` (the configured keys only). Use
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
