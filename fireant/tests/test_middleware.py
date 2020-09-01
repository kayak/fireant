from unittest import TestCase

from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware
from fireant.middleware.decorators import connection_middleware, QueryCancelled


class TestThreadPoolConcurrencyMiddleware(TestCase):
    @patch("fireant.middleware.concurrency.ThreadPool", autospec=True)
    def test_multiple_queries_execute_in_threadpool(self, mock_threadpool_manager):
        queries = ["query_a", "query_b"]
        mock_database = MagicMock()
        mock_database.fetch_dataframes.side_effect = [["result_a"], ["result_b"]]

        def mock_map(func, iterable):
            return [func(args) for args in iterable]

        pool_mock = (
            mock_threadpool_manager.return_value.__enter__.return_value
        ) = MagicMock()
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

    def test_keyboard_interrupt_tries_cancelling_query(self):
        mock_connection = MagicMock()
        mock_database_object = MagicMock()
        mock_database_object.connect.return_value.__enter__.return_value = mock_connection
        mock_function = MagicMock()
        mock_function.side_effect = KeyboardInterrupt()

        decorated_function = connection_middleware(mock_function)

        with self.assertRaises(QueryCancelled):
            decorated_function(mock_database_object, "query")

        mock_connection.cancel.assert_called_once()
        mock_connection.close.assert_called_once()
