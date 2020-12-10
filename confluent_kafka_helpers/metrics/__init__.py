from os import getenv

base_metric = 'confluent_kafka_helpers'


class StatsdNullClient:
    """
    No-op datadog statsd client implementing the null object pattern.
    """

    __call__ = __getattr__ = lambda self, *_, **__: self

    def timed(self, *args, **kwargs):
        return TimedNullDecorator()


class TimedNullDecorator:
    __enter__ = __getattr__ = lambda self, *_, **__: self

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def __exit__(self, *args):
        pass


try:
    if getenv('DATADOG_ENABLE_METRICS') != '1':
        statsd = StatsdNullClient()  # type: ignore
    else:
        import datadog

        statsd = datadog.statsd
except ModuleNotFoundError:
    statsd = StatsdNullClient()  # type: ignore
