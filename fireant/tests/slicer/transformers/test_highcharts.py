# coding: utf-8
from datetime import date
from unittest import TestCase

import numpy as np
import pandas as pd

from fireant.slicer import Slicer, Metric, ContinuousDimension, DatetimeDimension, CategoricalDimension, UniqueDimension
from fireant.slicer.transformers import (HighchartsLineTransformer, HighchartsColumnTransformer,
                                         HighchartsBarTransformer)
from fireant.slicer.transformers import highcharts, TransformationException
from fireant.tests import mock_dataframes as mock_df
from fireant.tests.database.mock_database import TestDatabase
from pypika import Table


class HighchartsLineTransformerTests(TestCase):
    """
    Line charts work with the following requests:

    1-cont-dim, *-metric
    1-cont-dim, *-dim, *-metric
    """

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsLineTransformer()

        test_table = Table('test_table')
        test_db = TestDatabase()
        cls.test_slicer = Slicer(
            table=test_table,
            database=test_db,

            dimensions=[
                ContinuousDimension('cont', definition=test_table.clicks),
                DatetimeDimension('date', definition=test_table.date),
                CategoricalDimension('cat', definition=test_table.cat),
                UniqueDimension('uni', definition=test_table.uni_id, display_field=test_table.uni_name),
            ],
            metrics=[Metric('foo')],
        )

    def evaluate_chart_options(self, result, num_series=1, xaxis_type='linear', dash_style='Solid'):
        self.assertSetEqual({'title', 'series', 'chart', 'tooltip', 'xAxis', 'yAxis'}, set(result.keys()))
        self.assertEqual(num_series, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(HighchartsLineTransformer.chart_type, result['chart']['type'])

        self.assertSetEqual({'type'}, set(result['xAxis'].keys()))
        self.assertEqual(xaxis_type, result['xAxis']['type'])

        for series in result['series']:
            self.assertSetEqual({'name', 'data', 'tooltip', 'yAxis', 'color', 'dashStyle'}, set(series.keys()))

    def evaluate_result(self, df, result):
        result_data = [series['data'] for series in result['series']]

        for data, (_, row) in zip(result_data, df.iteritems()):
            self.assertListEqual(list(row.iteritems()), data)

    def evaluate_tooltip_options(self, series, prefix=None, suffix=None, precision=None):
        self.assertIn('tooltip', series)

        tooltip = series['tooltip']
        if prefix is not None:
            self.assertIn('valuePrefix', tooltip)
            self.assertEqual(prefix, tooltip['valuePrefix'])
        if suffix is not None:
            self.assertIn('valueSuffix', tooltip)
            self.assertEqual(suffix, tooltip['valueSuffix'])
        if precision is not None:
            self.assertIn('valueDecimals', tooltip)
            self.assertEqual(precision, tooltip['valueDecimals'])

        else:
            self.assertSetEqual({'type'}, set(series['xAxis'].keys()))

    def test_require_dimensions(self):
        with self.assertRaises(TransformationException):
            self.hc_tx.prevalidate_request(self.test_slicer, [], [], [], [], [], [])

    def test_require_continuous_first_dimension(self):
        # A ContinuousDimension type is required for the first dimension
        self.hc_tx.prevalidate_request(self.test_slicer, [], ['cont'], [], [], [], [])
        self.hc_tx.prevalidate_request(self.test_slicer, [], ['date'], [], [], [], [])

        with self.assertRaises(TransformationException):
            self.hc_tx.prevalidate_request(self.test_slicer, [], ['cat'], [], [], [], [])
        with self.assertRaises(TransformationException):
            self.hc_tx.prevalidate_request(self.test_slicer, [], ['uni'], [], [], [], [])

    def test_series_single_metric(self):
        # Tests transformation of a single-metric, single-dimension result
        df = mock_df.cont_dim_single_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_dim_single_metric_schema)

        self.evaluate_chart_options(result)

        self.assertSetEqual(
            {'One'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_series_multi_metric(self):
        # Tests transformation of a multi-metric, single-dimension result
        df = mock_df.cont_dim_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_dim_multi_metric_schema)

        self.evaluate_chart_options(result, num_series=2)

        self.assertSetEqual(
            {'One', 'Two'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_time_series_date_to_millis(self):
        # Tests transformation of a single-metric, single-dimension result
        df = mock_df.time_dim_single_metric_df

        result = self.hc_tx.transform(df, mock_df.time_dim_single_metric_schema)

        self.evaluate_chart_options(result, xaxis_type='datetime')

        self.assertSetEqual(
            {'One'},
            {series['name'] for series in result['series']}
        )

        df2 = df.copy()
        df2.index = df2.index.astype(int) // int(1e6)
        self.evaluate_result(df2, result)

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        df = mock_df.time_dim_single_metric_ref_df

        result = self.hc_tx.transform(df, mock_df.time_dim_single_metric_ref_schema)

        self.evaluate_chart_options(result, num_series=2, xaxis_type='datetime')

        self.assertSetEqual(
            {'One', 'One WoW'},
            {series['name'] for series in result['series']}
        )

        df2 = df.copy()
        df2.index = df2.index.astype(int) // int(1e6)
        self.evaluate_result(df2, result)

    def test_cont_uni_dim_single_metric(self):
        # Tests transformation of a metric and a unique dimension
        df = mock_df.cont_uni_dims_single_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_uni_dims_single_metric_schema)

        self.evaluate_chart_options(result, num_series=3)

        self.assertSetEqual(
            {'One (Aa)', 'One (Bb)', 'One (Cc)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=[1, 2]), result)

    def test_cont_uni_dim_multi_metric(self):
        # Tests transformation of two metrics and a unique dimension
        df = mock_df.cont_uni_dims_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_uni_dims_multi_metric_schema)

        self.evaluate_chart_options(result, num_series=6)

        self.assertSetEqual(
            {'One (Aa)', 'One (Bb)', 'One (Cc)', 'Two (Aa)', 'Two (Bb)', 'Two (Cc)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=[1, 2]), result)

    def test_double_dimension_single_metric(self):
        # Tests transformation of a single-metric, double-dimension result
        df = mock_df.cont_cat_dims_single_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_cat_dims_single_metric_schema)

        self.evaluate_chart_options(result, num_series=2)

        self.assertSetEqual(
            {'One (A)', 'One (B)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=1), result)

    def test_double_dimension_multi_metric(self):
        # Tests transformation of a multi-metric, double-dimension result
        df = mock_df.cont_cat_dims_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_cat_dims_multi_metric_schema)

        self.evaluate_chart_options(result, num_series=4)

        self.assertSetEqual(
            {'One (A)', 'One (B)', 'Two (A)', 'Two (B)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=1), result)

    def test_triple_dimension_multi_metric(self):
        # Tests transformation of a multi-metric, double-dimension result
        df = mock_df.cont_cat_cat_dims_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_cat_cat_dims_multi_metric_schema)

        self.evaluate_chart_options(result, num_series=8)

        self.assertSetEqual(
            {'One (A, Y)', 'One (A, Z)', 'One (B, Y)', 'One (B, Z)',
             'Two (A, Y)', 'Two (A, Z)', 'Two (B, Y)', 'Two (B, Z)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=[1, 2]), result)

    def test_rollup_triple_dimension_multi_metric(self):
        # Tests transformation of a multi-metric, double-dimension result
        df = mock_df.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.evaluate_chart_options(result, num_series=14)

        self.assertSetEqual(
            {'One', 'One (A)', 'One (A, Y)', 'One (A, Z)', 'One (B)', 'One (B, Y)', 'One (B, Z)',
             'Two', 'Two (A)', 'Two (A, Y)', 'Two (A, Z)', 'Two (B)', 'Two (B, Y)', 'Two (B, Z)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=[1, 2]), result)

    def test_cont_dim_pretty(self):
        # Tests transformation of two metrics and a unique dimension
        df = mock_df.cont_dim_pretty_df

        result = self.hc_tx.transform(df, mock_df.cont_dim_pretty_schema)

        self.evaluate_chart_options(result)

        self.assertSetEqual(
            {'One'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_tooltip_options(result['series'][0], prefix='!', suffix='~', precision=1)
        self.evaluate_result(df, result)


class HighchartsColumnTransformerTests(TestCase):
    """
    Bar and Column charts work with the following requests:

    1-dim, *-metric
    2-dim, 1-metric
    """
    type = HighchartsColumnTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsColumnTransformer()

    def evaluate_chart_options(self, result, num_results=1, categories=None):
        self.assertSetEqual({'title', 'series', 'chart', 'tooltip', 'xAxis', 'yAxis'}, set(result.keys()))
        self.assertEqual(num_results, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(self.type, result['chart']['type'])
        self.assertEqual('categorical', result['xAxis']['type'])

        if categories:
            self.assertSetEqual({'type', 'categories'}, set(result['xAxis'].keys()))

        for series in result['series']:
            self.assertSetEqual({'name', 'data', 'yAxis', 'color', 'tooltip'}, set(series.keys()))

    def evaluate_tooltip_options(self, series, prefix=None, suffix=None, precision=None):
        self.assertIn('tooltip', series)

        tooltip = series['tooltip']
        if prefix is not None:
            self.assertIn('valuePrefix', tooltip)
            self.assertEqual(prefix, tooltip['valuePrefix'])
        if suffix is not None:
            self.assertIn('valueSuffix', tooltip)
            self.assertEqual(suffix, tooltip['valueSuffix'])
        if precision is not None:
            self.assertIn('valueDecimals', tooltip)
            self.assertEqual(precision, tooltip['valueDecimals'])

        else:
            self.assertSetEqual({'type'}, set(series['xAxis'].keys()))

    def evaluate_result(self, df, result):
        result_data = [series['data'] for series in result['series']]

        for data, (_, item) in zip(result_data, df.iteritems()):
            self.assertListEqual(list(item.iteritems()), data)

    def test_no_dims_multi_metric(self):
        df = mock_df.no_dims_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.no_dims_multi_metric_schema)

        self.evaluate_chart_options(result, num_results=8)

        self.assertSetEqual(
            {'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_cat_dim_single_metric(self):
        # Tests transformation of a single-metric, single-dimension result
        df = mock_df.cat_dim_single_metric_df

        result = self.hc_tx.transform(df, mock_df.cat_dim_single_metric_schema)

        self.evaluate_chart_options(result, categories=['A', 'B'])

        self.assertSetEqual(
            {'One'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_cat_dim_multi_metric(self):
        # Tests transformation of a single-metric, single-dimension result
        df = mock_df.cat_dim_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.cat_dim_multi_metric_schema)

        self.evaluate_chart_options(result, num_results=2, categories=['A', 'B'])

        self.assertSetEqual(
            {'One', 'Two'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_cat_cat_dim_single_metric(self):
        # Tests transformation of a multi-metric, single-dimension result
        df = mock_df.cat_cat_dims_single_metric_df

        result = self.hc_tx.transform(df, mock_df.cat_cat_dims_single_metric_schema)

        self.evaluate_chart_options(result, num_results=2, categories=['A', 'B'])

        self.assertSetEqual(
            {'One (Y)', 'One (Z)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(), result)

    def test_uni_dim_single_metric(self):
        # Tests transformation of a metric and a unique dimension
        df = mock_df.uni_dim_single_metric_df

        result = self.hc_tx.transform(df, mock_df.uni_dim_single_metric_schema)

        self.evaluate_chart_options(result, categories=['Uni_1', 'Uni_2', 'Uni_3'])

        self.assertSetEqual(
            {'One'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_uni_dim_multi_metric(self):
        # Tests transformation of two metrics and a unique dimension
        df = mock_df.uni_dim_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.uni_dim_multi_metric_schema)

        self.evaluate_chart_options(result, num_results=2, categories=['Uni_1', 'Uni_2', 'Uni_3'])

        self.assertSetEqual(
            {'One', 'Two'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df, result)

    def test_cont_dim_pretty(self):
        # Tests transformation of two metrics and a unique dimension
        df = mock_df.cont_dim_pretty_df

        result = self.hc_tx.transform(df, mock_df.cont_dim_pretty_schema)

        self.evaluate_chart_options(result)

        self.assertSetEqual(
            {'One'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_tooltip_options(result['series'][0], prefix='!', suffix='~', precision=1)
        self.evaluate_result(df, result)


class HighchartsBarTransformerTests(HighchartsColumnTransformerTests):
    type = HighchartsBarTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsBarTransformer()


class HighchartsUtilityTests(TestCase):
    def test_str_data_point(self):
        result = highcharts._format_data_point('abc')
        self.assertEqual('abc', result)

    def test_int64_data_point(self):
        # Needs to be cast to python int
        result = highcharts._format_data_point(np.int64(1))
        self.assertEqual(int(1), result)

    def test_datetime_data_point(self):
        # Needs to be converted to milliseconds
        result = highcharts._format_data_point(pd.Timestamp(date(2000, 1, 1)))
        self.assertEqual(946684800000, result)

    def test_nan_data_point(self):
        # Needs to be cast to python int
        result = highcharts._format_data_point(np.nan)
        self.assertIsNone(result)
