from unittest import TestCase

import fireant as f
from fireant.slicer.exceptions import MetricRequiredException
from fireant.slicer.queries.builder import add_hints
from pypika import (
    MySQLQuery,
    VerticaQuery,
)
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
                .queries

    def test_datatablesjs_requires_at_least_one_metric(self):
        with self.assertRaises(TypeError):
            slicer.data \
                .widget(f.DataTablesJS())


class QueryHintsTests(TestCase):
    def test_add_hint_to_query_if_supported_by_dialect_and_hint_is_set(self):
        query = VerticaQuery.from_('table').select('*')
        query_hint = add_hints([query], 'test_hint')
        self.assertEqual('SELECT /*+label(test_hint)*/ * FROM "table"', str(query_hint[0]))

    def test_do_not_add_hints_to_query_if_not_supported_by_dialect(self):
        query = MySQLQuery.from_('table').select('*')
        query_hint = add_hints([query], 'test_hint')
        self.assertEqual('SELECT * FROM `table`', str(query_hint[0]))

    def test_do_not_add_hints_to_query_if_no_hint_string_supplied(self):
        query = VerticaQuery.from_('table').select('*')
        query_hint = add_hints([query], hint=None)
        self.assertEqual('SELECT * FROM "table"', str(query_hint[0]))
