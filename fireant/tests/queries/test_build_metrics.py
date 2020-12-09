from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricTests(TestCase):
    maxDiff = None

    def test_build_query_with_single_metric(self):
        queries = mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes)).sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT ' 'SUM("votes") "$votes" ' 'FROM "politics"."politician" ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_multiple_metrics(self):
        queries = mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes, mock_dataset.fields.wins)).sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_multiple_visualizations(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .widget(f.ReactTable(mock_dataset.fields.wins))
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_for_chart_visualization_with_single_axis(self):
        queries = mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))).sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT ' 'SUM("votes") "$votes" ' 'FROM "politics"."politician" ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_for_chart_visualization_with_multiple_axes(self):
        queries = mock_dataset.query.widget(
            f.HighCharts()
            .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))
            .axis(f.HighCharts.LineSeries(mock_dataset.fields.wins))
        ).sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )
