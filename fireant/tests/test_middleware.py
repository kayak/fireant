from unittest import TestCase

from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from fireant.middleware.concurrency import (
    BaseConcurrencyMiddleware,
    ThreadPoolConcurrencyMiddleware,
)


class TestThreadPoolConcurrencyMiddleware(TestCase):
    @patch.object(BaseConcurrencyMiddleware, '__abstractmethods__', set())
    def test_single_query_executes_synchronously(self):
        query = 'query'
        mock_database = MagicMock()
        mock_database.fetch.return_value = 'result'

        concurrency_middleware = ThreadPoolConcurrencyMiddleware()

        result = concurrency_middleware.fetch_query(query, mock_database)

        self.assertEqual(result, 'result')
        mock_database.fetch.assert_called_with(query)

    @patch('fireant.middleware.concurrency.ThreadPool', autospec=True)
    def test_multiple_queries_execute_in_threadpool(self, mock_threadpool_manager):
        queries = ['query_a', 'query_b']
        mock_database = MagicMock()
        mock_database.fetch_dataframe.side_effect = ['result_a', 'result_b']

        def mock_map(func, iterable):
            return [func(args) for args in iterable]

        pool_mock = mock_threadpool_manager.return_value.__enter__.return_value = MagicMock()
        pool_mock.map = mock_map

        middleware = ThreadPoolConcurrencyMiddleware()

        results = middleware.fetch_queries_as_dataframe(queries, mock_database)

        self.assertEqual(results, ['result_a', 'result_b'])
        mock_database.fetch_dataframe.assert_has_calls([call('query_a'), call('query_b')])
