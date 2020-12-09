from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset

timestamp_daily = f.day(mock_dataset.fields.timestamp)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderOperationTests(TestCase):
    maxDiff = None

    def test_build_query_with_cumsum_operation(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(f.CumSum(mock_dataset.fields.votes))).dimension(timestamp_daily).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_cummean_operation(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(f.CumMean(mock_dataset.fields.votes))).dimension(timestamp_daily).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_cumprod_operation(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(f.CumProd(mock_dataset.fields.votes))).dimension(timestamp_daily).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_rollingmean_operation(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(f.RollingMean(mock_dataset.fields.votes, 3, 3)))
            .dimension(timestamp_daily)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )
