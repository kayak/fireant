# coding: utf-8
import numpy as np
import pandas as pd

from unittest import TestCase
from pypika import Table

from fireant.slicer import Slicer, Metric, CategoricalDimension, SlicerException
from fireant.slicer.operations import Totals, CumSum, L1Loss
from fireant.tests.database.mock_database import TestDatabase


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
        self.assertEqual('_total', totals.key)

    def test_data_query_schema__totals_dim_set_in_rollup(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo', 'bar'],
            dimensions=['dim'],
            operations=[Totals('dim')],
        )
        self.assertListEqual(query_schema['rollup'], [['dim']])

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
        self.assertDictEqual(display_schema['metrics'], {'foo': {'label': 'Foo', 'axis': 0},
                                                         'bar': {'label': 'Bar', 'axis': 1}})
        self.assertDictEqual(display_schema['dimensions'], {
            'dim': {'label': 'Dim', 'display_options': {np.nan: '', pd.NaT: ''}}
        })
        self.assertDictEqual(display_schema['references'], {})


class MetricOperationTests(TestCase):
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

    def test_metric_included_in_query_schema_for_operation(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['dim'],
            operations=[CumSum('foo')],
        )

        # Metric included
        self.assertTrue('foo' in query_schema['metrics'])

        # No rollup added
        self.assertEqual(len(query_schema['rollup']), 0)

    def test_metric_included_in_query_schema_for_operation_without_metrics(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=[],
            dimensions=['dim'],
            operations=[CumSum('foo')],
        )

        # Metric included
        self.assertTrue('foo' in query_schema['metrics'])

    def test_all_metrics_included_in_query_schema_for_operation_without_metrics(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=[],
            dimensions=['dim'],
            operations=[L1Loss('foo','bar')],
        )

        # Metric included
        self.assertTrue('foo' in query_schema['metrics'])
        self.assertTrue('bar' in query_schema['metrics'])

