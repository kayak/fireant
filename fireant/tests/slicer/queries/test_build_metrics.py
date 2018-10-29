from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricTests(TestCase):
    maxDiff = None

    def test_build_query_with_single_metric(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician"', str(queries[0]))

    def test_build_query_with_multiple_metrics(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes, slicer.metrics.wins)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician"', str(queries[0]))

    def test_build_query_with_multiple_visualizations(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .widget(f.DataTablesJS(slicer.metrics.wins)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician"', str(queries[0]))

    def test_build_query_for_chart_visualization_with_single_axis(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician"', str(queries[0]))

    def test_build_query_for_chart_visualization_with_multiple_axes(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))
                    .axis(f.HighCharts.LineSeries(slicer.metrics.wins))) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician"', str(queries[0]))
