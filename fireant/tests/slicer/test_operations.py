# coding: utf-8
from unittest import TestCase

from fireant.slicer import Slicer, Metric, CategoricalDimension, SlicerException
from fireant.slicer.operations import Totals
from fireant.tests.database.mock_database import TestDatabase
from pypika import Table


class TotalsTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_table = Table('test_table')
        cls.test_slicer = Slicer(
            table=Table('test_table'),
            database=TestDatabase(),
            joins=[],
            metrics=[
                Metric('foo'),
                Metric('bar'),
            ],

            dimensions=[
                CategoricalDimension('dim'),
            ]
        )

    def test_totals_init(self):
        totals = Totals('date')
        self.assertEqual('totals', totals.key)

    def test_data_query_schema__totals_dim_set_in_rollup(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo', 'bar'],
            dimensions=['dim'],
            operations=[Totals('dim')],
        )
        self.assertListEqual(query_schema['rollup'], ['dim'])

    def test_data_query_schema__exception_when_missing_totals_dim(self):
        with self.assertRaises(SlicerException):
            print(self.test_slicer.manager.data_query_schema(
                metrics=['foo', 'bar'],
                dimensions=[],
                operations=[Totals('dim')],
            ))

    def test_totals_display_schema__no_extra_metrics_selected(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo', 'bar'],
            dimensions=['dim'],
            operations=[Totals('dim')]
        )
        self.assertDictEqual(display_schema['metrics'], {'foo': {'label': 'Foo'}, 'bar': {'label': 'Bar'}})
        self.assertDictEqual(display_schema['dimensions'], {'dim': {'label': 'Dim', 'display_options': {}}})
        self.assertDictEqual(display_schema['references'], {})
