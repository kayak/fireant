from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware, BaseConcurrencyMiddleware


class TestThreadPoolConcurrencyMiddleware(TestCase):

    @patch('fireant.middleware.concurrency.ThreadPool', autospec=True)
    def test_fetch_queries_as_dataframe(self, mock_threadpool_manager):
        queries = ['query_a', 'query_b']
        database = 'database'
        pool_mock = MagicMock()
        pool_mock.map.return_value = ['result_a', 'result_b']
        mock_threadpool_manager.return_value.__enter__.return_value = pool_mock

        middleware = ThreadPoolConcurrencyMiddleware()

        results = middleware.fetch_queries_as_dataframe(queries, database)

        self.assertEqual(results, ['result_a', 'result_b'])
        pool_mock.map.assert_called_with(ANY, [('query_a', 'database'), ('query_b', 'database')])


class TestBaseConcurrencyMiddleware(TestCase):

    @patch.object(BaseConcurrencyMiddleware, '__abstractmethods__', set())
    def test_fetch_query(self):
        query = 'query'
        mock_database = MagicMock()
        mock_database.fetch.return_value = 'result'

        concurrency_middleware = BaseConcurrencyMiddleware()

        result = concurrency_middleware.fetch_query(query, mock_database)

        self.assertEqual(result, 'result')
        mock_database.fetch.assert_called_with(query)

