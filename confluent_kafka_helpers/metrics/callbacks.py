from functools import partial

from confluent_kafka_helpers.metrics import base_metric, statsd
from confluent_kafka_helpers.metrics.stats import StatsParser


def error_cb_metrics(error, statsd=statsd):
    statsd.event("Kafka error", str(error), alert_type='error')


def on_delivery_cb_metrics(error, message, statsd=statsd):
    statsd.increment(f'{base_metric}.producer.message.count.total')
    if error:
        statsd.increment(f'{base_metric}.producer.message.count.error')


class StatsCallbackMetrics:
    """
    Send librdkafka statistics to Datadog.

    For more information see:
    https://github.com/edenhill/librdkafka/wiki/Statistics
    """
    def __init__(self, stats, parser=StatsParser, statsd=statsd):
        self.stats = parser(stats)
        self.statsd = statsd
        self.base_tags = [f'instance_type:{self.stats.type}']

        self._send_stats()
        self._send_broker_stats()
        self._send_topic_partition_stats()
        self._send_cgrp_stats()

    def _send_stats(self):
        base = f'{base_metric}.librdkafka'
        gauge = partial(self.statsd.gauge, tags=self.base_tags)

        gauge(f'{base}.replyq', self.stats.replyq)
        gauge(f'{base}.msg_cnt', self.stats.msg_cnt)
        gauge(f'{base}.msg_size', self.stats.msg_size)
        gauge(f'{base}.msg_max', self.stats.msg_max)
        gauge(f'{base}.msg_size_mac', self.stats.msg_size_mac)

    def _send_broker_stats(self):
        base = f'{base_metric}.librdkafka.broker'
        for b in self.stats.brokers:
            tags = self.base_tags + [f'broker:{b.name}']
            gauge = partial(self.statsd.gauge, tags=tags)

            gauge(f'{base}.outbuf_cnt', b.outbuf_cnt)
            gauge(f'{base}.outbuf_msg_cnt', b.outbuf_msg_cnt)
            gauge(f'{base}.waitresp_cnt', b.waitresp_cnt)
            gauge(f'{base}.waitresp_msg_cnt', b.waitresp_msg_cnt)
            gauge(f'{base}.tx', b.tx)
            gauge(f'{base}.txbytes', b.txbytes)
            gauge(f'{base}.txerrs', b.txerrs)
            gauge(f'{base}.txretries', b.txretries)
            gauge(f'{base}.req_timeouts', b.req_timeouts)
            gauge(f'{base}.rx', b.rx)
            gauge(f'{base}.rxbytes', b.rxbytes)
            gauge(f'{base}.rxerrs', b.rxerrs)
            gauge(f'{base}.rxcorriderrs', b.rxcorriderrs)
            gauge(f'{base}.rxpartial', b.rxpartial)
            gauge(f'{base}.zbuf_grow', b.zbuf_grow)
            gauge(f'{base}.buf_grow', b.buf_grow)
            gauge(f'{base}.wakeups', b.wakeups)
            gauge(f'{base}.int_latency.min', b.int_latency['min'])
            gauge(f'{base}.int_latency.max', b.int_latency['max'])
            gauge(f'{base}.int_latency.avg', b.int_latency['avg'])
            gauge(f'{base}.int_latency.sum', b.int_latency['sum'])
            gauge(f'{base}.int_latency.cnt', b.int_latency['cnt'])
            gauge(f'{base}.rtt.min', b.rtt['min'])
            gauge(f'{base}.rtt.max', b.rtt['max'])
            gauge(f'{base}.rtt.avg', b.rtt['avg'])
            gauge(f'{base}.rtt.sum', b.rtt['sum'])
            gauge(f'{base}.rtt.cnt', b.rtt['cnt'])
            gauge(f'{base}.throttle.min', b.throttle['min'])
            gauge(f'{base}.throttle.max', b.throttle['max'])
            gauge(f'{base}.throttle.avg', b.throttle['avg'])
            gauge(f'{base}.throttle.sum', b.throttle['sum'])
            gauge(f'{base}.throttle.cnt', b.throttle['cnt'])

    def _send_topic_partition_stats(self):
        base = f'{base_metric}.librdkafka.topic'
        for topic in self.stats.topics:
            for p in topic.partitions:
                tags = self.base_tags + [
                    f'topic:{topic.topic}', f'partition:{p.partition}'
                ]
                gauge = partial(self.statsd.gauge, tags=tags)

                gauge(f'{base}.msgq_cnt', p.msgq_cnt)
                gauge(f'{base}.msgq_bytes', p.msgq_bytes)
                gauge(f'{base}.xmit_msgq_cnt', p.xmit_msgq_cnt)
                gauge(f'{base}.xmit_msgq_bytes', p.xmit_msgq_bytes)
                gauge(f'{base}.fetchq_cnt', p.fetchq_cnt)
                gauge(f'{base}.fetchq_size', p.fetchq_size)
                gauge(f'{base}.query_offset', p.query_offset)
                gauge(f'{base}.next_offset', p.next_offset)
                gauge(f'{base}.app_offset', p.app_offset)
                gauge(f'{base}.stored_offset', p.stored_offset)
                gauge(f'{base}.committed_offset', p.committed_offset)
                gauge(f'{base}.lo_offset', p.lo_offset)
                gauge(f'{base}.hi_offset', p.hi_offset)
                gauge(f'{base}.consumer_lag', p.consumer_lag)
                gauge(f'{base}.txmsgs', p.txmsgs)
                gauge(f'{base}.txbytes', p.txbytes)
                gauge(f'{base}.msgs', p.msgs)
                gauge(f'{base}.rx_ver_drops', p.rx_ver_drops)

    def _send_cgrp_stats(self):
        cgrp = self.stats.cgrp
        if not cgrp:
            return

        base = f'{base_metric}.librdkafka.cgrp'
        gauge = partial(self.statsd.gauge, tags=self.base_tags)
        gauge(f'{base}.rebalance_age', cgrp['rebalance_age'])
        gauge(f'{base}.rebalance_cnt', cgrp['rebalance_cnt'])
        gauge(f'{base}.assignment_size', cgrp['assignment_size'])
