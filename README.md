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

`is_shutdown_requested()` is the public API for application and worker layers to
observe graceful shutdown. Long-running handlers can check the flag and bail out
early between subtasks:

```python
from confluent_kafka_helpers import is_shutdown_requested

for message in consumer:
    do_step_one(message)
    if is_shutdown_requested():
        break
    do_step_two(message)
```

Batch schedulers should also check `is_shutdown_requested()` before starting
queued work. Once it returns `True`, do not start new message handlers; leave
unfinished work for Kafka redelivery.

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

`AvroBatchConsumer` is a separate, opt-in iterator of nonempty lists of messages:

```python
from confluent_kafka_helpers.consumer import AvroBatchConsumer

with AvroBatchConsumer(config, batch_size=100, timeout=0.1) as consumer:
    for batch in consumer:
        with producer.delivery_scope(timeout=30.0):
            bulk_write([message.value for message in batch])
            producer.produce_many(
                {"value": build_event(message), "source": message}
                for message in batch
            )
        consumer.commit_batch(batch)
```

Usually you pass the handler to eventsourcing_helpers (`consume_batch(handle_batch)`), which
coordinates output delivery and commits after successful handling.

See the [batch migration guide](docs/migration_guides/v1.md#opt-in-to-batch-consumption)
for configuration changes, existing-handler usage and replay rules.

- Each native `consume()` read returns up to `batch_size` messages (default 100).
  `timeout` bounds the read in seconds (default 0.1), including when idle. Empty reads are skipped.
  The size must be an integer from 1 to 1,000,000; timeout must be finite and nonnegative.
- Batch iteration stops reading when shutdown has already been requested. If
  shutdown is requested while a native `consume()` call is in progress, records
  returned by that read are not yielded and remain unprocessed for Kafka
  redelivery.
- With `stop_on_eof`, finish all assigned partitions, yield remaining data, then stop.
- The batch consumer disables automatic commits and offset storage, rejecting settings that
  enable either. It rejects `get_message`, `poll_timeout`, `non_blocking` and the previous
  experimental `batch_max_size` / `batch_max_wait` keys. Use constructor arguments instead.
- `commit_batch(messages)` synchronously commits the highest supplied offset plus one per
  topic-partition. Supply only successful prefixes per partition: committing a later record
  acknowledges earlier records too. Partition errors and assignment loss propagate without retry.
  Stop on fetch, processing, delivery or commit failure and let Kafka replay uncommitted input.
- Use `AvroConsumer` for single-message iteration; its API remains unchanged.
- Preserve record order within each partition when handling batches. Kafka has no global order
  across partitions, and parallel handlers can reorder effects even within a partition.
- **Handlers must be idempotent.** Delivery is at-least-once per batch: if the process crashes,
  the handler raises, the commit fails or a rebalance happens before the commit, the whole batch
  is delivered again, including messages whose db writes were already committed.

### Propagated headers

The consumer config `headers.propagate` lists header keys (e.g. `correlation_id`) that
`producer.produce()` forwards on the messages it produces. When consuming one message at a time,
this is automatic while the message is handled.

When consuming in batches, wrap each message handler in `message.process()`. Do this inside the
worker that handles the message. Then every produced message from that handler gets the right
headers.

```python
def handle_message(message):
    with message.process():
        producer.produce(build_event(message))


def handle_batch(batch):
    for message in batch:
        handle_message(message)
```

`produce()` and `produce_many()` only queue output. Wrap processing in
`producer.delivery_scope(timeout=30.0)` before committing input. The scope tracks **all** calls
on that producer, including ordinary `produce()` calls from existing handlers. It preserves
configured and per-record callbacks, bounds queue waits and confirms delivery before successful
exit. Delivery failures, callback failures, timeouts or outstanding output prevent acknowledgement.
The timeout starts at scope entry and covers queue waits and delivery confirmation; it cannot
interrupt application work or schema-registry requests. Join workers before leaving the scope.
Do not share that producer with unrelated work during a scope; nested scopes are rejected.
Delivery scopes reject `delivery.report.only.error=True`, since confirming delivery requires
successful delivery callbacks as well as failures. Ordinary publishing still allows this setting.

`produce_many(records, timeout=30.0)` queues ordinary asynchronous `produce()` calls in supplied
order, allowing librdkafka to batch network traffic while preserving headers. Outside a scope,
its timeout bounds queue-space waits, not delivery. It retries only the rejected record on a full
queue. A scope uses its own shared deadline instead.

For collected bulk results, keep the input message and pass `source=message` to `produce()` or
each `produce_many` entry. This selects its propagation snapshot and incoming trace without an
ambient message scope. Explicit business headers override propagated headers with the same key.
Without a source or per-message scope, output inherits the batch trace but no input's business
headers. Producer idempotence does not deduplicate application replay or make database writes
and Kafka publication atomic; applications must make both effects replay-safe.

### Tracing

Each batch gets one `kafka.consume` span. It starts a new trace and links to every message's
trace context. This shows the whole batch.

Each `message.process()` block starts one `kafka.process` span for one message. Work inside the
block, such as database writes or OpenSearch requests, belongs to that message's trace.

For threaded handlers, put `message.process()` inside the worker function:

```python
from concurrent.futures import ThreadPoolExecutor


def handle_message(message):
    with message.process():
        save_to_database(message.value)
        producer.produce(build_projection_event(message.value))


def handle_batch(batch):
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(handle_message, batch))
```

OpenTelemetry keeps at most 128 links per span by default and silently drops the rest. If you raise
`batch_size` above 128, also raise `OTEL_SPAN_LINK_COUNT_LIMIT`.

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
