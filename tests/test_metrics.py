from confluent_kafka_helpers.metrics import StatsdNullClient


class StatsdNullClientTests:
    def setup_method(self):
        self.client = StatsdNullClient()

    def test_method_calls_should_not_fail(self):
        self.client.increment('foo', 1)
        self.client.foo('baz', 2)

    def test_timed_decorator_should_execute_decorated_method(self):
        @self.client.timed('foo', 1)
        def foo(a):
            return a

        value = foo('bar')
        assert value == 'bar'
