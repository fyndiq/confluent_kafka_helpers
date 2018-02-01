import ujson

from confluent_kafka_helpers.metrics.stats import StatsParser

stats = ujson.dumps({
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
    }
})


class StatsParserTests:
    def test_should_parse_stats_str(self):
        parsed = StatsParser(stats)

        assert parsed.brokers[0].name == 'b1'
        assert parsed.topics[0].topic == 't1'
        assert parsed.topics[0].partitions[0].partition == 'p1'
