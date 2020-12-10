import structlog

from confluent_kafka_helpers.metrics import base_metric, statsd

logger = structlog.get_logger(__name__)


def send_metric(base, metric, stats, tags):
    if not stats:
        return
    value = stats.get(metric)
    name = f'{base}.{metric}'
    statsd.gauge(name, value, tags=tags)


def send_top_level_stats(stats, base_tags):
    base = f'{base_metric}.librdkafka'
    metrics = [
        'replyq',
        'msg_cnt',
        'msg_size',
        'msg_max',
        'msg_size_max',
        'tx',
        'tx_bytes',
        'rx',
        'rx_bytes',
        'txmsgs',
        'txmsg_bytes',
        'rxmsgs',
        'rxmsg_bytes',
        'metadata_cache_cnt',
    ]
    for metric in metrics:
        send_metric(base, metric, stats, base_tags)


def send_broker_stats(stats, base_tags):
    base = f'{base_metric}.librdkafka.broker'
    metrics = [
        'stateage',
        'outbuf_cnt',
        'outbuf_msg_cnt',
        'waitresp_cnt',
        'waitresp_msg_cnt',
        'tx',
        'txbytes',
        'txerrs',
        'txretries',
        'req_timeouts',
        'rx',
        'rxbytes',
        'rxerrs',
        'rxcorriderrs',
        'rxpartial',
        'zbuf_grow',
        'buf_grow',
        'wakeups',
        'connects',
        'disconnects',
    ]
    window_metrics = [
        'min',
        'max',
        'avg',
        'sum',
        'cnt',
    ]
    for name, broker in stats.get('brokers', {}).items():
        if broker.get('nodeid') == -1:  # bootstrap servers
            continue
        tags = base_tags + [f'broker:{name}']
        for metric in metrics:
            send_metric(base, metric, broker, tags)
        for metric in window_metrics:
            int_latency, rtt, throttle = (
                broker.get('int_latency'),
                broker.get('rtt'),
                broker.get('throttle'),
            )
            send_metric(f'{base}.int_latency', metric, int_latency, tags)
            send_metric(f'{base}.rtt', metric, rtt, tags)
            send_metric(f'{base}.throttle', metric, throttle, tags)


def send_cgrp_stats(stats, base_tags):
    base = f'{base_metric}.librdkafka.cgrp'
    metrics = [
        'stateage',
        'rebalance_age',
        'rebalance_cnt',
        'assignment_size',
    ]
    for metric in metrics:
        send_metric(base, metric, stats.get('cgrp'), base_tags)
