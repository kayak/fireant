from unittest import TestCase

import fireant as f
from fireant.slicer.exceptions import MetricRequiredException
from ..mocks import slicer


class QueryBuilderTests(TestCase):
    def test_widget_is_immutable(self):
        query1 = slicer.data
        query2 = query1.widget(f.DataTablesJS(slicer.metrics.votes))

        self.assertIsNot(query1, query2)

    def test_dimension_is_immutable(self):
        query1 = slicer.data
        query2 = query1.dimension(slicer.dimensions.timestamp)

        self.assertIsNot(query1, query2)

    def test_filter_is_immutable(self):
        query1 = slicer.data
        query2 = query1.filter(slicer.dimensions.timestamp == 'ok')

        self.assertIsNot(query1, query2)

    def test_orderby_is_immutable(self):
        query1 = slicer.data
        query2 = query1.orderby(slicer.dimensions.timestamp)

        self.assertIsNot(query1, query2)




# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderValidationTests(TestCase):
    maxDiff = None

    def test_highcharts_requires_at_least_one_axis(self):
        with self.assertRaises(MetricRequiredException):
            slicer.data \
                .widget(f.HighCharts()) \
                .dimension(slicer.dimensions.timestamp) \
                .query

    def test_datatablesjs_requires_at_least_one_metric(self):
        with self.assertRaises(TypeError):
            slicer.data \
                .widget(f.DataTablesJS())

