from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_metric_filter_eq(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields.votes == 5)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_eq_left(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(5 == mock_dataset.fields.votes)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_ne(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields.votes != 5)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")<>5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_ne_left(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(5 != mock_dataset.fields.votes)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")<>5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_gt(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes)).filter(mock_dataset.fields.votes > 5).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")>5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_gt_left(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes)).filter(5 < mock_dataset.fields.votes).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")>5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_gte(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields.votes >= 5)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")>=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_gte_left(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(5 <= mock_dataset.fields.votes)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")>=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_lt(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes)).filter(mock_dataset.fields.votes < 5).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")<5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_lt_left(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes)).filter(5 > mock_dataset.fields.votes).sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")<5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_lte(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields.votes <= 5)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")<=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_metric_filter_lte_left(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(5 >= mock_dataset.fields.votes)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'HAVING SUM("votes")<=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )
