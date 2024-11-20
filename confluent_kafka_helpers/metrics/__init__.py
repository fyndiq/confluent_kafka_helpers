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


class StatsdConsoleClient:
    """
    Print metrics to the console for debugging.
    """

    def __call__(self, *args, **kwargs):
        print(args, kwargs)
        return self

    def __getattr__(self, name):
        def method(*args, **kwargs):
            print(args, kwargs)
            return self

        return method


try:
    if getenv('DATADOG_ENABLE_METRICS') != '1':
        if getenv('DATADOG_METRICS_DEBUG') == '1':
            statsd = StatsdConsoleClient()  # type: ignore
        else:
            statsd = StatsdNullClient()  # type: ignore
    else:
        import datadog

        statsd = datadog.statsd
except ModuleNotFoundError:
    statsd = StatsdNullClient()  # type: ignore
