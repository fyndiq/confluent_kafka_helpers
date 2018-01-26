class Broker:
    def __init__(self, broker):
        self._broker = broker

    def __getattr__(self, attr):
        return self._broker[attr]

    def __repr__(self):
        return f'<Broker: {self.name}>'


class Brokers(list):
    def __init__(self, brokers):
        for key, value in brokers.items():
            self.append(Broker(value))

    def __repr__(self):
        brokers = ', '.join([repr(b) for b in self])
        return f'[{brokers}]'
