class EndOfPartition(Exception):
    """We have reached the end of a partition"""


class KafkaTransportError(Exception):
    """
    Kafka transport errors:
        - GroupCoordinator response error: Local: Broker transport failure
    """


class KafkaError(Exception):
    pass


class KafkaDeliveryError(KafkaError):
    def __init__(self, error: str, message):
        self.error = error
        self.message = message
