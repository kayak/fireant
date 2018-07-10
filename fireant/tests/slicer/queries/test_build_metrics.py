from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricTests(TestCase):
    maxDiff = None

    def test_build_query_with_single_metric(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_with_multiple_metrics(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes, slicer.metrics.wins)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_with_multiple_visualizations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .widget(f.DataTablesJS(slicer.metrics.wins)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_for_chart_visualization_with_single_axis(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_for_chart_visualization_with_multiple_axes(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))
                    .axis(f.HighCharts.LineChart(slicer.metrics.wins))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician"', str(query))
