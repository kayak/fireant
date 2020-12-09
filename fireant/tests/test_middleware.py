import signal
from unittest import TestCase
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from fireant.exceptions import QueryCancelled
from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware
from fireant.middleware.decorators import CancelableConnection, connection_middleware


class TestThreadPoolConcurrencyMiddleware(TestCase):
    @patch("fireant.middleware.concurrency.ThreadPool", autospec=True)
    def test_multiple_queries_execute_in_threadpool(self, mock_threadpool_manager):
        queries = ["query_a", "query_b"]
        mock_database = MagicMock()
        mock_database.fetch_dataframes.side_effect = [["result_a"], ["result_b"]]

        def mock_map(func, iterable):
            return [func(args) for args in iterable]

        pool_mock = mock_threadpool_manager.return_value.__enter__.return_value = MagicMock()
        pool_mock.map = mock_map

        middleware = ThreadPoolConcurrencyMiddleware(max_processes=2)

        results = middleware(mock_database.fetch_dataframes)(mock_database, *queries)

        self.assertEqual(["result_a", "result_b"], results)
        mock_database.fetch_dataframes.assert_has_calls(
            [call(mock_database, "query_a"), call(mock_database, "query_b")]
        )
        mock_threadpool_manager.assert_called_with(processes=2)


class TestConnectionMiddleware(TestCase):
    def test_decorator_provides_connection_if_non_provided(self):
        mock_connection = MagicMock()
        mock_database_object = MagicMock()
        mock_database_object.connect.return_value.__enter__.return_value = mock_connection
        mock_function = MagicMock()

        decorated_function = connection_middleware(mock_function)
        decorated_function(mock_database_object, "query")

        mock_function.assert_called_once_with(mock_database_object, "query", connection=mock_connection)

    @patch("fireant.middleware.decorators.signal.signal")
    def test_cancelable_connection_attaches_signal_handlers(self, mock_attach_signal):
        mock_connection = MagicMock()
        mock_database_object = MagicMock()
        mock_database_object.connect.return_value.__enter__.return_value = mock_connection

        cancelable_connection_manager = CancelableConnection(mock_database_object)
        with cancelable_connection_manager:
            pass

        mock_attach_signal.assert_has_calls(
            [
                call(signal.SIGINT, cancelable_connection_manager._handle_interrupt_signal),
                call(signal.SIGINT, signal.default_int_handler),
            ]
        )

    @patch("fireant.middleware.decorators.signal.signal")
    def test_cancelable_connection_transforms_keyboard_intterupt_in_cancelled_query(self, mock_attach_signal):
        mock_connection = MagicMock()
        mock_database_object = MagicMock()
        mock_database_object.connect.return_value.__enter__.return_value = mock_connection

        cancelable_connection_manager = CancelableConnection(mock_database_object)
        with self.assertRaises(QueryCancelled):
            with cancelable_connection_manager:
                raise KeyboardInterrupt()

        mock_attach_signal.assert_has_calls(
            [
                call(signal.SIGINT, cancelable_connection_manager._handle_interrupt_signal),
                call(signal.SIGINT, signal.default_int_handler),
            ]
        )

    @patch("fireant.middleware.decorators.time")
    def test_cancelable_connection_sleeps_if_wait_time_after_close_is_set(self, mock_time):
        mock_connection = MagicMock()
        mock_database_object = MagicMock()
        mock_database_object.connect.return_value.__enter__.return_value = mock_connection

        cancelable_connection_manager = CancelableConnection(mock_database_object, wait_time_after_close=5)
        with cancelable_connection_manager:
            pass

        mock_time.sleep.assert_called_once_with(5)
