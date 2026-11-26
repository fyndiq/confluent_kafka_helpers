# Migrating to Kafka batch processing

Install `confluent-kafka-helpers==1.8.0rc3` to try the batch API with the
published prerelease. Choose one mode per processing loop. Use
`AvroConsumer` for single-message iteration or `AvroBatchConsumer` for
batch iteration.

## Consume, publish, and commit

`AvroBatchConsumer(config, batch_size=100, timeout=0.1)` accepts an integer batch
size from 1 through 1,000,000. Each fetch waits up to `timeout` seconds and can
return a smaller batch. The iterator skips empty fetches. The timeout must be
finite and nonnegative. The consumer disables automatic commits and offset
storage, and rejects settings that enable either.

Pass `error_handler=` to handle Kafka fetch errors other than partition EOF.
A handler that returns allows the loop to skip that error record. Batch iteration uses Kafka's
native fetch and rejects the single-message options `get_message`,
`poll_timeout`, and `non_blocking`.

This example forwards a topic to another topic with compatible Avro key and
value schemas. Set the environment variables to your Kafka and Schema Registry
endpoints, input and output topics, and a consumer group for the forwarder.

```python
from os import environ

from confluent_kafka_helpers.consumer import AvroBatchConsumer
from confluent_kafka_helpers.producer import AvroProducer

settings = {
    "bootstrap.servers": environ["KAFKA_BROKERS"],
    "schema.registry.url": environ["SCHEMA_REGISTRY_URL"],
}
input_topic = environ["INPUT_TOPIC"]
output_topic = environ["OUTPUT_TOPIC"]
producer = AvroProducer({**settings, "topics": [output_topic]})

with AvroBatchConsumer(
    {**settings, "topics": [input_topic], "group.id": environ["CONSUMER_GROUP"]},
    batch_size=100,
    timeout=0.1,
) as consumer:
    for batch in consumer:
        with producer.delivery_scope(timeout=30.0):
            producer.produce_many(
                {
                    "key": message._meta.key,
                    "value": message.value,
                    "topic": output_topic,
                    "source": message,
                }
                for message in batch
            )
        consumer.commit_batch(batch)
```

`produce_many(records, *, timeout=30.0)` accepts the same record arguments as
`produce(...)`. Its timeout bounds waits for queue space outside a delivery
scope. Inside `producer.delivery_scope(timeout=30.0)`, both methods use the
same deadline, starting with the first output. The scope checks delivery
callbacks before exiting successfully. Scopes for different producers are
independent. Opening a second scope on the same producer raises
`RuntimeError`. Backends that call `finish_producing(flush=...)` can use the
same code in both modes. Outside the scope this serves callbacks and
optionally flushes. Inside the scope it defers to delivery confirmation.

Commit only after all application work and output delivery succeed.
`commit_batch(messages)` commits
the highest processed offset plus one per topic and partition synchronously.
Pass the processed prefix of each partition so a commit cannot skip pending
work. Treat a fetch, processing, delivery, or commit error as a reason to stop
and let the consumer group resume from committed offsets.

## Headers and trace context

Set `source=message` on `produce(...)` or in each `produce_many` record to copy
the source's configured propagated headers and associate the output span with
its trace context. Explicit output headers take precedence over propagated
business headers. The helper adds batch span links in input order up to
OpenTelemetry's `OTEL_SPAN_LINK_COUNT_LIMIT` (128 by default). An SDK may
apply a lower limit of its own. This cap leaves each record's source trace
available for output spans.

For existing handlers that call `produce(...)` themselves, run each call inside
`with message_context(message):` using
`confluent_kafka_helpers.context.message_context`. It activates that record's
propagated headers and trace context, then restores the previous context on
exit. Passing `source=None` or omitting it uses the active context. Outside a
per-message scope, this uses the batch span and inherits no input record's
business headers.

## Ordering and replay

Kafka preserves order within each partition. The consumer yields records in
fetch order, and `produce_many` queues records in the supplied order. Records
from different partitions or topics can interleave without a global order.

Processing is at least once. A failed commit or a crash after database or
output work can replay input. Applications must make state changes and output
effects safe to repeat. Kafka producer idempotence covers producer retries.
Application replay requires its own idempotency, such as a stable input ID and
output identity for downstream deduplication.
