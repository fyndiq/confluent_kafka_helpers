import structlog

from confluent_kafka_helpers.metrics import base_metric, statsd

logger = structlog.get_logger(__name__)
window_metrics = ['min', 'max', 'avg', 'sum', 'cnt', 'p95']


def send_metric(base, metric, stats, tags):
    if not stats:
        return
    value = stats.get(metric)
    name = f'{base}.{metric.lower()}'
    statsd.gauge(name, value, tags=tags)


def send_top_level_stats(stats, base_tags):
    base = f'{base_metric}.librdkafka'
    metrics = [
        # Number of ops (callbacks, events, etc) waiting in queue for application to serve with
        # rd_kafka_poll()
        'replyq',
        # Current number of messages in producer queues
        'msg_cnt',
        # Threshold: maximum number of messages allowed allowed on the producer queues
        'msg_max',
        # Current total size of messages in producer queues
        'msg_size',
        # Threshold: maximum total size of messages allowed on the producer queues
        'msg_size_max',
        # Total number of requests sent to Kafka brokers
        'tx',
        # Total number of bytes transmitted to Kafka brokers
        'tx_bytes',
        # Total number of responses received from Kafka brokers
        'rx',
        # Total number of bytes received from Kafka brokers
        'rx_bytes',
        # Total number of messages transmitted (produced) to Kafka brokers
        'txmsgs',
        # Total number of message bytes (including framing, such as per-Message framing and
        # MessageSet/batch framing) transmitted to Kafka brokers
        'txmsg_bytes',
        # Total number of messages consumed, not including ignored messages (due to offset, etc),
        # from Kafka brokers.
        'rxmsgs',
        # Total number of message bytes (including framing) received from Kafka brokers
        'rxmsg_bytes',
    ]
    for metric in metrics:
        send_metric(base, metric, stats, base_tags)


def send_broker_stats(stats, base_tags):
    base = f'{base_metric}.librdkafka.broker'
    metrics = [
        # Time since last broker state change (microseconds)
        'stateage',
        # Number of requests awaiting transmission to broker
        'outbuf_cnt',
        # Number of messages awaiting transmission to broker
        'outbuf_msg_cnt',
        # Number of requests in-flight to broker awaiting response
        'waitresp_cnt',
        # Number of messages in-flight to broker awaiting response
        'waitresp_msg_cnt',
        # Total number of requests sent
        'tx',
        # Total number of bytes sent
        'txbytes',
        # Total number of transmission errors
        'txerrs',
        # Total number of request retries
        'txretries',
        # Microseconds since last socket send (or -1 if no sends yet for current connection).
        'txidle',
        # Total number of requests timed out
        'req_timeouts',
        # Total number of responses received
        'rx',
        # Total number of bytes received
        'rxbytes',
        # Total number of receive errors
        'rxerrs',
        # Total number of unmatched correlation ids in response (typically for timed out requests)
        'rxcorriderrs',
        # Total number of partial MessageSets received. The broker may return partial responses if
        # the full MessageSet could not fit in the remaining Fetch response size.
        'rxpartial',
        # Microseconds since last socket receive (or -1 if no receives yet for current connection).
        'rxidle',
        # Total number of decompression buffer size increases
        'zbuf_grow',
        # Broker thread poll loop wakeups
        'wakeups',
        # Number of connection attempts, including successful and failed, and name resolution
        # failures.
        'connects',
        # Number of disconnects (triggered by broker, network, load-balancer, etc.).
        'disconnects',
    ]
    req_names = [
        'AddOffsetsToTxn',
        'AddPartitionsToTxn',
        'ApiVersion',
        'DescribeCluster',
        'DescribeProducers',
        'DescribeTransactions',
        'EndTxn',
        'FindCoordinator',
        'GetTelemetrySubscriptions',
        'InitProducerId',
        'ListOffsets',
        'ListTransactions',
        'Metadata',
        'Produce',
        'PushTelemetry',
        'SaslAuthenticate',
        'SaslHandshake',
        'TxnOffsetCommit',
    ]
    for name, broker in stats.get('brokers', {}).items():
        if broker.get('nodeid') == -1:  # bootstrap servers
            continue
        tags = base_tags + [f'broker:{name}']
        for metric in metrics:
            send_metric(base, metric, broker, tags)
        for metric in window_metrics:
            int_latency, outbuf_latency, rtt, throttle = (
                broker.get('int_latency'),
                broker.get('outbuf_latency'),
                broker.get('rtt'),
                broker.get('throttle'),
            )
            send_metric(f'{base}.int_latency', metric, int_latency, tags)
            send_metric(f'{base}.outbuf_latency', metric, outbuf_latency, tags)
            send_metric(f'{base}.rtt', metric, rtt, tags)
            send_metric(f'{base}.throttle', metric, throttle, tags)
        # Request type counters. Object key is the request name, value is the number of requests
        # sent.
        if req := broker.get('req'):
            for metric in req_names:
                send_metric(f'{base}.req', metric, req, tags)


def send_cgrp_stats(stats, base_tags):
    base = f'{base_metric}.librdkafka.cgrp'
    metrics = [
        # Time elapsed since last state change (milliseconds).
        'stateage',
        # Time elapsed since last rebalance (assign or revoke) (milliseconds).
        'rebalance_age',
        # Total number of rebalances (assign or revoke).
        'rebalance_cnt',
        # Current assignment's partition count.
        'assignment_size',
    ]
    for metric in metrics:
        send_metric(base, metric, stats.get('cgrp'), base_tags)


def send_topics_stats(stats, base_tags):
    topics = stats.get('topics')
    if not topics:
        return

    base = f'{base_metric}.librdkafka.topics'
    for name, topic in topics.items():
        tags = base_tags + [f'topic:{name}']
        for metric in window_metrics:
            batchsize, batchcnt = (
                # Batch sizes in bytes
                topic.get('batchsize'),
                # Batch message counts
                topic.get('batchcnt'),
            )
            send_metric(f'{base}.batchsize', metric, batchsize, tags)
            send_metric(f'{base}.batchcnt', metric, batchcnt, tags)
