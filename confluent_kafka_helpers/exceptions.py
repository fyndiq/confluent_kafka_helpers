class EndOfPartition(Exception):
    """ We have reached the end of a partition """


class KafkaTransportError(Exception):
    """
    Kafka transport errors:
        - GroupCoordinator response error: Local: Broker transport failure
    """
