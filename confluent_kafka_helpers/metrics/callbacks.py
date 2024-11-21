import json

import structlog

from confluent_kafka_helpers.metrics import base_metric, statsd
from confluent_kafka_helpers.metrics.statistics import (
    send_broker_stats,
    send_cgrp_stats,
    send_top_level_stats,
    send_topics_stats,
)

logger = structlog.get_logger(__name__)


def error_cb_metrics(error, statsd=statsd):
    logger.info("Error callback triggered")
    statsd.event("Kafka error", str(error), alert_type='error')


def on_delivery_cb_metrics(error, message, statsd=statsd):
    logger.info("On delivery callback triggered")
    statsd.increment(f'{base_metric}.producer.message.count.total')
    if error:
        statsd.increment(f'{base_metric}.producer.message.count.error')


def stats_cb_metrics(stats):
    logger.info("Stats callback triggered")
    stats = json.loads(stats)
    instance_type = stats.get('type')
    client_id = stats.get('client_id')
    base_tags = [f'type:{instance_type}', f'client_id:{client_id}']

    send_top_level_stats(stats, base_tags)
    send_broker_stats(stats, base_tags)
    send_cgrp_stats(stats, base_tags)
    send_topics_stats(stats, base_tags)
