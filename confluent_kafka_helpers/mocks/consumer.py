from confluent_kafka_helpers.mocks.kafka import Broker


class MockAvroConsumer:
    def __init__(self, *args, broker: Broker = Broker):
        self._broker = broker()

    def poll(self, *args, **kwargfs):
        self._broker.get_message()

    def subscribe(self, *args, **kwargs):
        pass

    def close(self):
        pass
