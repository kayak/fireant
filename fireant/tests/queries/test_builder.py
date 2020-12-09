from unittest import TestCase

from pypika import (
    MySQLQuery,
    VerticaQuery,
)

import fireant as f
from fireant.queries.builder import add_hints
from fireant.tests.dataset.mocks import mock_dataset
from fireant.widgets.base import MetricRequiredException


class QueryBuilderTests(TestCase):
    def test_widget_is_immutable(self):
        query1 = mock_dataset.query
        query2 = query1.widget(f.ReactTable(mock_dataset.fields.votes))

        self.assertIsNot(query1, query2)

    def test_dimension_is_immutable(self):
        query1 = mock_dataset.query
        query2 = query1.dimension(mock_dataset.fields.timestamp)

        self.assertIsNot(query1, query2)

    def test_filter_is_immutable(self):
        query1 = mock_dataset.query
        query2 = query1.filter(mock_dataset.fields.timestamp == 'ok')

        self.assertIsNot(query1, query2)

    def test_orderby_is_immutable(self):
        query1 = mock_dataset.query
        query2 = query1.orderby(mock_dataset.fields.timestamp)

        self.assertIsNot(query1, query2)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderValidationTests(TestCase):
    maxDiff = None

    def test_highcharts_requires_at_least_one_axis(self):
        with self.assertRaises(MetricRequiredException):
            mock_dataset.query.widget(f.HighCharts()).dimension(mock_dataset.fields.timestamp).sql

    def test_ReactTable_requires_at_least_one_metric(self):
        with self.assertRaises(TypeError):
            mock_dataset.query.widget(f.ReactTable())


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
