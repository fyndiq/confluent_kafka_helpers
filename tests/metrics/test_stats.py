import json

from confluent_kafka_helpers.metrics.stats import StatsParser

stats = {
    'brokers': {
        'b1': {
            'name': 'b1'
        }
    },
    'topics': {
        't1': {
            'topic': 't1',
            'partitions': {
                'p1': {
                    'partition': 'p1'
                }
            }
        }
    },
    'cgrp': {
        'foo': 'bar'
    }
}


class StatsParserTests:
    def test_get_top_level_attrs_should_return_str(self):
        parsed = StatsParser(json.dumps(stats))

        assert parsed.brokers[0].name == 'b1'
        assert parsed.topics[0].topic == 't1'
        assert parsed.topics[0].partitions[0].partition == 'p1'
        assert parsed.cgrp['foo'] == 'bar'

    def test_get_cgrp_attr_should_return_none(self):
        stats.pop('cgrp')
        parsed = StatsParser(json.dumps(stats))
        assert parsed.cgrp is None
