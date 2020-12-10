from unittest.mock import Mock

import pytest

from confluent_kafka_helpers.utils import retry_exception


class TestRetryException:
    def test_maximum_retries_and_reraises_exception(self):
        @retry_exception([ValueError])
        def foo(mock):
            raise mock()

        mock = Mock(side_effect=ValueError)
        with pytest.raises(ValueError):
            foo(mock)
        assert mock.call_count == 3

    def test_retry_once_and_return(self):
        @retry_exception([ZeroDivisionError])
        def foo(mock):
            return 2 / mock()

        mock = Mock(side_effect=[0, 1])
        value = foo(mock)
        assert mock.call_count == 2
        assert value == 2
