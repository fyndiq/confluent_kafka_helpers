import opentracing


def get_producer_tags(topic: str, key: str) -> dict:
    return {
        opentracing.tags.SPAN_KIND: opentracing.tags.SPAN_KIND_PRODUCER,
        opentracing.tags.COMPONENT: 'messagebus',
        opentracing.tags.PEER_SERVICE: 'kafka',
        opentracing.tags.MESSAGE_BUS_DESTINATION: topic,
        'message_bus.key': key,
    }


def get_consumer_tags(topic: str, key: str) -> dict:
    return {
        opentracing.tags.SPAN_KIND: opentracing.tags.
        SPAN_KIND_CONSUMER,
        opentracing.tags.COMPONENT: 'messagebus',
        opentracing.tags.PEER_SERVICE: 'kafka',
        opentracing.tags.MESSAGE_BUS_DESTINATION: topic,
        'message_bus.key': key
    }
