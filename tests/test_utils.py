from unittest.mock import Mock, call, patch

import pytest

from confluent_kafka_helpers.utils import retry_exception


class TestRetryException:
    @pytest.mark.parametrize("delay,expected", [(0, []), (0.1, [call(0.1), call(0.2)])])
    def test_backoff_is_opt_in_and_only_between_attempts(self, delay, expected):
        failure = ValueError("failed")
        operation = Mock(side_effect=failure)
        wrapped = retry_exception([ValueError], delay=delay)(operation)
        with patch("confluent_kafka_helpers.utils.time.sleep") as sleep:
            with pytest.raises(ValueError) as raised:
                wrapped()
        assert raised.value is failure
        assert operation.call_count == 3
        assert sleep.call_args_list == expected

    def test_backoff_respects_initial_delay(self):
        operation = Mock(side_effect=ValueError)
        wrapped = retry_exception([ValueError], delay=2)(operation)
        with patch("confluent_kafka_helpers.utils.time.sleep") as sleep:
            with pytest.raises(ValueError):
                wrapped()
        assert sleep.call_args_list == [call(2), call(4)]

    def test_does_not_delay_permanent_errors(self):
        wrapped = retry_exception([ValueError], condition=lambda exc: False, delay=0.1)(
            Mock(side_effect=ValueError)
        )
        with patch("confluent_kafka_helpers.utils.time.sleep") as sleep:
            with pytest.raises(ValueError):
                wrapped()
        sleep.assert_not_called()

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
