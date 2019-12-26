from unittest import TestCase

from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware


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
