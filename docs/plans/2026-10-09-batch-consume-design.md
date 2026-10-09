# Batch consume design

## Goal

Let handlers receive messages in batches of configurable size (e.g. for bulk DB writes).
Raw consume throughput is not a goal: librdkafka already fetches in batches, and Avro decoding
dominates per-message cost. If `poll()` overhead is ever measured as the bottleneck, the internals
can switch to `Consumer.consume(num_messages)` without changing the public API.

## API

New method on the existing `AvroConsumer`. Configured through the consumer config dict, like
`poll_timeout` and `stop_on_eof`:

```python
consumer = AvroConsumer({
    "bootstrap.servers": ...,
    "group.id": ...,
    "topics": [...],
    "batch_max_size": 100,  # default 100
    "batch_max_wait": 1.0,  # seconds, default 1.0
})

for batch in consumer.batches():  # list[Message]
    bulk_write(batch)
    consumer.commit(asynchronous=False)
```

- Both keys are popped in `__init__`. librdkafka rejects unknown properties.
- No per-call override.
- Mixing `batches()` and per-message iteration on one consumer is unsupported (documented, not enforced).

## Batching behaviour

`batches()` loops over the existing `self._get_message()`, so retries, error handler and
`poll_timeout` work as they do today. Each raw message is wrapped in `Message` and buffered.
The buffer is yielded when either:

- `len(buffer) == batch_max_size`, or
- `batch_max_wait` has elapsed since the **first** buffered message (buffer non-empty).

The timer starts at the first message, so an idle consumer yields nothing, and a message waits
at most about `batch_max_wait + poll_timeout`. Use `time.monotonic()`.

Edge cases:

| Case | Behaviour |
|---|---|
| Shutdown requested | Yield partial buffer, then stop |
| `stop_on_eof` and EOF | Yield partial buffer, then stop |
| `non_blocking=True`, no messages | Yield `[]` after `batch_max_wait` |
| Error handler raises | Propagate; buffered messages are dropped (not committed, so redelivered) |

Committing stays the caller's job. `commit(asynchronous=False)` with no arguments commits the
current position, which covers the whole batch. This gives at-least-once delivery per batch.

## Tracing

One `kafka.consume` span (kind `CONSUMER`) per batch, open while the batch is yielded,
the same pattern as the per-message generator.

- No parent context; the span starts a new trace.
- Links: the combined `tracer.extract_links(tracer.extract_headers(msg._meta.headers))` results for every message.
- Attributes kept: operation name/type (`consume`/`receive`), client id, consumer group, server address/port.
- New attribute: `messaging.batch.message_count` (new constant in `tracing/attributes.py`).
- `messaging.destination.name`: set only when all messages share one topic.
- `resource_name`: topic, or sorted topics joined with `,`.
- Dropped: partition, offset, key, message class, propagated-header attributes.
- Propagated headers are not set. Callers read `msg._meta.headers`. Call `clear_propagated_headers()` before each yield.

OTEL limits links to 128 per span (`OTEL_SPAN_LINK_COUNT_LIMIT`) and silently drops the rest. The
default `batch_max_size=100` stays under it. Document the limit for apps that raise the size.

## Metrics

Increment `consumer.message.count.total` by `len(batch)`.

## Testing

In `tests/test_consumer.py`, reusing `avro_consumer`, `tracer` and the shutdown-flag reset fixture.
Script `poll()` results; patch `time.monotonic` instead of sleeping.

- 250 messages, size 100 → batches of 100, 100, 50 (last via `max_wait` flush).
- Partial batch yielded after `max_wait` since first message.
- Idle: blocking yields nothing; `non_blocking` yields `[]` after `max_wait`.
- Batch keys are popped and not passed to the underlying consumer.
- Shutdown mid-batch and `stop_on_eof` EOF yield the partial batch, then stop.
- Error-handler exception propagates; buffered messages not yielded.
- One span per batch: no parent, `messaging.batch.message_count`, one link per traced message.
- `destination.name` present for single-topic batch, absent for mixed.
- No propagated headers set.
- `consumer.message.count.total` incremented by batch length.

## Docs

README section on batch consumption: config keys, commit-per-batch example, OTEL link limit.
