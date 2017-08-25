# coding: utf-8
from datetime import date
from unittest import TestCase

import numpy as np
import pandas as pd
from pypika import Table

from fireant.slicer import (
    BooleanDimension,
    CategoricalDimension,
    ContinuousDimension,
    DatetimeDimension,
    Metric,
    Slicer,
    UniqueDimension,
)
from fireant.slicer.transformers import (
    HighchartsAreaPercentageTransformer,
    HighchartsAreaTransformer,
    HighchartsBarTransformer,
    HighchartsColumnTransformer,
    HighchartsLineTransformer,
    HighchartsPieTransformer,
    HighchartsStackedBarTransformer,
    HighchartsStackedColumnTransformer,
    TransformationException,
    highcharts,
)
from fireant.tests import mock_dataframes as mock_df
from fireant.tests.database.mock_database import TestDatabase


class BaseHighchartsTransformerTests(TestCase):
    """
    Test methods that are common to all Highcharts Transformers
    """

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


class HighchartsLineTransformerTests(BaseHighchartsTransformerTests):
    """
    Line charts work with the following requests:

    1-cont-dim, *-metric
    1-cont-dim, *-dim, *-metric
    """
    chart_type = HighchartsLineTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsLineTransformer()

        test_table = Table('test_table')
        test_db = TestDatabase()
        cls.test_slicer = Slicer(
            table=test_table,
            database=test_db,

            dimensions=[
                BooleanDimension('bool', definition=test_table.clicks < 10),
                CategoricalDimension('cat', definition=test_table.cat),
                ContinuousDimension('cont', definition=test_table.clicks),
                DatetimeDimension('date', definition=test_table.date),
                UniqueDimension('uni', definition=test_table.uni_id, display_field=test_table.uni_name),
            ],
            metrics=[Metric('foo')],
        )

    def evaluate_chart_options(self, result, num_series=1, xaxis_type='linear', dash_style='Solid'):
        self.assertSetEqual({'title', 'series', 'chart', 'plotOptions', 'tooltip', 'xAxis', 'yAxis'},
                            set(result.keys()))
        self.assertEqual(num_series, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(self.chart_type, result['chart']['type'])

        self.assertSetEqual({'type'}, set(result['xAxis'].keys()))
        self.assertEqual(xaxis_type, result['xAxis']['type'])

        for series in result['series']:
            self.assertSetEqual({'name', 'data', 'tooltip', 'yAxis', 'color', 'dashStyle'}, set(series.keys()))

    def evaluate_result(self, df, result):
        result_data = [series['data'] for series in result['series']]

        for data, (_, row) in zip(result_data, df.iteritems()):
            self.assertListEqual(list(row.iteritems()), data)

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

    def test_cont_bool_dim_single_metric(self):
        # Tests transformation of a metric and a boolean dimension
        df = mock_df.cont_bool_dims_single_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_bool_dims_single_metric_schema)

        self.evaluate_chart_options(result, num_series=3)

        self.assertSetEqual(
            {'One (Null)', 'One (False)', 'One (True)'},
            {series['name'] for series in result['series']}
        )
        self.evaluate_result(df.unstack(level=1), result)

    def test_cont_bool_dim_multi_metric(self):
        # Tests transformation of two metrics and a boolean dimension
        df = mock_df.cont_bool_dims_multi_metric_df

        result = self.hc_tx.transform(df, mock_df.cont_bool_dims_multi_metric_schema)

        self.evaluate_chart_options(result, num_series=6)

        self.assertSetEqual(
            {'One (Null)', 'One (False)', 'One (True)', 'Two (Null)', 'Two (False)', 'Two (True)'},
            {series['name'] for series in result['series']}
        )

        self.evaluate_result(df.unstack(level=[1]), result)

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


class HighchartsAreaTransformerTests(HighchartsLineTransformerTests):
    chart_type = HighchartsAreaTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        super(HighchartsAreaTransformerTests, cls).setUpClass()
        cls.hc_tx = HighchartsAreaTransformer()

    def evaluate_chart_options(self, result, num_series=1, xaxis_type='linear', dash_style='Solid'):
        self.assertSetEqual({'title', 'series', 'chart', 'plotOptions', 'tooltip', 'xAxis', 'yAxis'},
                            set(result.keys()))
        self.assertEqual(num_series, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(self.chart_type, result['chart']['type'])

        self.assertSetEqual({'type'}, set(result['xAxis'].keys()))
        self.assertEqual(xaxis_type, result['xAxis']['type'])

        for series in result['series']:
            self.assertSetEqual({'name', 'data', 'tooltip', 'color', 'dashStyle'}, set(series.keys()))


class HighchartsAreaPercentageTransformerTests(HighchartsAreaTransformerTests):
    chart_type = HighchartsAreaPercentageTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        super(HighchartsAreaPercentageTransformerTests, cls).setUpClass()
        cls.hc_tx = HighchartsAreaPercentageTransformer()


class HighchartsColumnTransformerTests(TestCase):
    """
    Bar and Column charts work with the following requests:

    1-dim, *-metric
    2-dim, 1-metric
    """
    chart_type = HighchartsColumnTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsColumnTransformer()

    def evaluate_chart_options(self, result, num_results=1, categories=None):
        self.assertSetEqual({'title', 'series', 'chart', 'tooltip', 'xAxis', 'yAxis', 'plotOptions'},
                            set(result.keys()))
        self.assertEqual(num_results, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(self.chart_type, result['chart']['type'])
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


class HighchartsStackedColumnTransformerTests(HighchartsColumnTransformerTests):
    chart_type = HighchartsStackedColumnTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsStackedColumnTransformer()

    def evaluate_chart_options(self, result, num_results=1, categories=None):
        self.assertSetEqual({'title', 'series', 'chart', 'tooltip', 'xAxis', 'yAxis', 'plotOptions'},
                            set(result.keys()))
        self.assertEqual(num_results, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(self.chart_type, result['chart']['type'])
        self.assertEqual('categorical', result['xAxis']['type'])

        if categories:
            self.assertSetEqual({'type', 'categories'}, set(result['xAxis'].keys()))

        for series in result['series']:
            self.assertSetEqual({'name', 'data', 'color', 'tooltip'}, set(series.keys()))


class HighchartsBarTransformerTests(HighchartsColumnTransformerTests):
    chart_type = HighchartsBarTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsBarTransformer()


class HighchartsStackedBarTransformerTests(HighchartsStackedColumnTransformerTests):
    chart_type = HighchartsStackedBarTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsStackedBarTransformer()


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


class HighChartsPieChartTests(BaseHighchartsTransformerTests):
    """
    Pie charts work with the following requests:

    1-metric, *-dim
    """
    type = HighchartsPieTransformer.chart_type

    @classmethod
    def setUpClass(cls):
        cls.hc_tx = HighchartsPieTransformer()

    def evaluate_chart_options(self, result, num_results=1):
        self.assertSetEqual({'title', 'series', 'chart', 'tooltip', 'plotOptions'}, set(result.keys()))
        self.assertEqual(num_results, len(result['series']))

        self.assertSetEqual({'text'}, set(result['title'].keys()))
        self.assertIsNone(result['title']['text'])

        self.assertEqual(self.type, result['chart']['type'])

        for series in result['series']:
            self.assertSetEqual({'name', 'data'}, set(series.keys()))

    def test_no_dims_single_metric(self):
        # Tests transformation of a single-metric, no-dimension result
        df = mock_df.no_dims_single_metric_df

        result = self.hc_tx.transform(df, mock_df.no_dims_single_metric_schema)
        self.evaluate_chart_options(result, num_results=1)
        self.assertEqual(result['series'][0], {'name': 'One', 'data': [('', 0.0)]})

    def test_cat_dim_single_metric(self):
        # Tests transformation of a single-metric, single-dimension result
        df = mock_df.cat_dim_single_metric_df
        result = self.hc_tx.transform(df, mock_df.cat_dim_single_metric_schema)
        result_series = result['series'][0]
        self.assertEqual(result_series, {'data': [('A', 0.0), ('B', 1.0)], 'name': 'One'})

    def test_uni_dim_single_metric(self):
        # Tests transformation of a single metric and a unique dimension
        df = mock_df.uni_dim_single_metric_df
        result = self.hc_tx.transform(df, mock_df.uni_dim_single_metric_schema)
        result_series = result['series'][0]
        self.assertEqual(result_series, {'data': [('Aa', 0.0), ('Bb', 1.0), ('Cc', 2.0)], 'name': 'One'})

    def test_date_dim_single_metric(self):
        # Tests transformation of a single metric and a datetime dimension
        df = mock_df.time_dim_single_metric_df
        result = self.hc_tx.transform(df, mock_df.time_dim_single_metric_schema)
        result_series = result['series'][0]
        self.assertEqual(result_series, {
            'data': [
                ('2000-01-01 00:00:00', 0),
                ('2000-01-02 00:00:00', 1),
                ('2000-01-03 00:00:00', 2),
                ('2000-01-04 00:00:00', 3),
                ('2000-01-05 00:00:00', 4),
                ('2000-01-06 00:00:00', 5),
                ('2000-01-07 00:00:00', 6),
                ('2000-01-08 00:00:00', 7),
            ], 'name': 'One'
        })

    def test_cat_cat_and_a_single_metric(self):
        # Tests transformation of two categorical dimensions with a single metric
        df = mock_df.cat_cat_dims_single_metric_df
        result = self.hc_tx.transform(df, mock_df.cat_cat_dims_single_metric_schema)
        result_series = result['series'][0]
        self.assertEqual(result_series,
                         {'data': [('(A, Y)', 0.0), ('(A, Z)', 1.0), ('(B, Y)', 2.0), ('(B, Z)', 3.0)], 'name': 'One'})

    def test_cont_cat_uni_and_a_single_metric(self):
        # Tests transformation of a categorical and unique dimensions with a single metric
        df = mock_df.cont_cat_uni_dims_multi_metric_df
        result = self.hc_tx.transform(df, mock_df.cont_cat_uni_dims_multi_metric_schema)
        result_series = result['series'][0]
        self.assertEqual(result_series,
                         {
                             'data': [('(0, A, Aa)', 0.0), ('(0, A, Bb)', 1.0), ('(0, A, Cc)', 2.0),
                                      ('(0, B, Aa)', 3.0),
                                      ('(0, B, Bb)', 4.0), ('(0, B, Cc)', 5.0), ('(1, A, Aa)', 6.0),
                                      ('(1, A, Bb)', 7.0),
                                      ('(1, A, Cc)', 8.0), ('(1, B, Aa)', 9.0), ('(1, B, Bb)', 10.0),
                                      ('(1, B, Cc)', 11.0),
                                      ('(2, A, Aa)', 12.0), ('(2, A, Bb)', 13.0), ('(2, A, Cc)', 14.0),
                                      ('(2, B, Aa)', 15.0), ('(2, B, Bb)', 16.0), ('(2, B, Cc)', 17.0),
                                      ('(3, A, Aa)', 18.0), ('(3, A, Bb)', 19.0), ('(3, A, Cc)', 20.0),
                                      ('(3, B, Aa)', 21.0), ('(3, B, Bb)', 22.0), ('(3, B, Cc)', 23.0),
                                      ('(4, A, Aa)', 24.0), ('(4, A, Bb)', 25.0), ('(4, A, Cc)', 26.0),
                                      ('(4, B, Aa)', 27.0), ('(4, B, Bb)', 28.0), ('(4, B, Cc)', 29.0),
                                      ('(5, A, Aa)', 30.0), ('(5, A, Bb)', 31.0), ('(5, A, Cc)', 32.0),
                                      ('(5, B, Aa)', 33.0), ('(5, B, Bb)', 34.0), ('(5, B, Cc)', 35.0),
                                      ('(6, A, Aa)', 36.0), ('(6, A, Bb)', 37.0), ('(6, A, Cc)', 38.0),
                                      ('(6, B, Aa)', 39.0), ('(6, B, Bb)', 40.0), ('(6, B, Cc)', 41.0),
                                      ('(7, A, Aa)', 42.0), ('(7, A, Bb)', 43.0), ('(7, A, Cc)', 44.0),
                                      ('(7, B, Aa)', 45.0), ('(7, B, Bb)', 46.0), ('(7, B, Cc)', 47.0)],
                             'name': 'One'
                         })

    def test_unique_dim_single_metric_pretty_tooltip(self):
        # Tests transformation of a single metrics and a unique dimension with correct tooltip
        df = mock_df.uni_dim_pretty_df
        result = self.hc_tx.transform(df, mock_df.uni_dim_pretty_schema)
        result_series = result['series'][0]
        self.assertEqual(result_series, {'data': [('Aa', 0.0), ('Bb', 1.0), ('Cc', 2.0)], 'name': 'One'})
        self.evaluate_tooltip_options(result, prefix='!', suffix='~', precision=1)
