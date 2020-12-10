import json

from confluent_kafka_helpers.metrics import base_metric, statsd
from confluent_kafka_helpers.metrics.statistics import (
    send_broker_stats,
    send_cgrp_stats,
    send_top_level_stats,
)


def error_cb_metrics(error, statsd=statsd):
    statsd.event("Kafka error", str(error), alert_type='error')


def on_delivery_cb_metrics(error, message, statsd=statsd):
    statsd.increment(f'{base_metric}.producer.message.count.total')
    if error:
        statsd.increment(f'{base_metric}.producer.message.count.error')


def stats_cb_metrics(stats):
    stats = json.loads(stats)
    instance_type = stats.get('type')
    base_tags = [f'type:{instance_type}']

    send_top_level_stats(stats, base_tags)
    send_broker_stats(stats, base_tags)
    send_cgrp_stats(stats, base_tags)
