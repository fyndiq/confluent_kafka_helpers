from unittest.mock import Mock, call, patch

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

    def test_condition_true_retries(self):
        @retry_exception([ValueError], condition=lambda exc: True)
        def foo(mock):
            raise mock()

        mock = Mock(side_effect=ValueError)
        with pytest.raises(ValueError):
            foo(mock)
        assert mock.call_count == 3

    def test_condition_false_does_not_retry(self):
        @retry_exception([ValueError], condition=lambda exc: False)
        def foo(mock):
            raise mock()

        mock = Mock(side_effect=ValueError)
        with pytest.raises(ValueError):
            foo(mock)
        assert mock.call_count == 1

    def test_condition_can_filter_by_exception_attribute(self):
        @retry_exception([ValueError], condition=lambda exc: "retry" in str(exc))
        def foo(mock):
            raise mock()

        retry_then_fatal = Mock(side_effect=[ValueError("retry me"), ValueError("fatal")])
        with pytest.raises(ValueError, match="fatal"):
            foo(retry_then_fatal)
        assert retry_then_fatal.call_count == 2

    def test_default_condition_retries_all(self):
        @retry_exception([ValueError])
        def foo(mock):
            raise mock()

        mock = Mock(side_effect=ValueError)
        with pytest.raises(ValueError):
            foo(mock)
        assert mock.call_count == 3

    def test_non_matching_exception_does_not_invoke_condition(self):
        condition = Mock(return_value=True)

        @retry_exception([ValueError], condition=condition)
        def foo():
            raise TypeError("not in list")

        with pytest.raises(TypeError):
            foo()
        condition.assert_not_called()

    @patch("confluent_kafka_helpers.utils.time.sleep")
    def test_no_backoff_by_default(self, sleep):
        @retry_exception([ValueError])
        def foo(mock):
            raise mock()

        with pytest.raises(ValueError):
            foo(Mock(side_effect=ValueError))
        sleep.assert_not_called()

    @patch("confluent_kafka_helpers.utils.time.sleep")
    def test_backoff_doubles_and_is_capped(self, sleep):
        @retry_exception([ValueError], retries=6, backoff=1, max_backoff=5)
        def foo(mock):
            raise mock()

        mock = Mock(side_effect=ValueError)
        with pytest.raises(ValueError):
            foo(mock)
        assert mock.call_count == 6
        assert sleep.call_args_list == [call(1), call(2), call(4), call(5), call(5)]

    @patch("confluent_kafka_helpers.utils.time.sleep")
    def test_backoff_not_applied_when_condition_false(self, sleep):
        @retry_exception([ValueError], condition=lambda exc: False, backoff=1)
        def foo(mock):
            raise mock()

        with pytest.raises(ValueError):
            foo(Mock(side_effect=ValueError))
        sleep.assert_not_called()
