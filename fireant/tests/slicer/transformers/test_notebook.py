# coding: utf-8
from datetime import datetime

import numpy as np
from mock import patch, ANY, call

from fireant.slicer.transformers import PandasRowIndexTransformer, TransformationException
from fireant.tests.slicer.transformers.base import BaseTransformerTests


class PandasRowIndexTransformerTests(BaseTransformerTests):
    pd_tx = PandasRowIndexTransformer()

    def test_no_dims_multi_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.pd_tx.transform(self.no_dims_multi_metric_df, self.no_dims_multi_metric_schema)

        self.assertListEqual([None], list(result.index.names))
        self.assertListEqual([0], list(result.index))

        self.assertListEqual(['One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight'], list(result.columns))
        self.assertListEqual([a for a in range(8)], result.values[0].tolist())

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.pd_tx.transform(self.cont_dim_single_metric_df, self.cont_dim_single_metric_schema)

        self.assertListEqual(['Cont'], list(result.index.names))
        self.assertListEqual([a for a in range(8)], list(result.index))

        self.assertListEqual(['One'], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result['One']))

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        result = self.pd_tx.transform(self.cont_dim_multi_metric_df, self.cont_dim_multi_metric_schema)

        self.assertListEqual(['Cont'], list(result.index.names))
        self.assertListEqual([a for a in range(8)], list(result.index))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result['One']))
        self.assertListEqual([2 * a for a in range(8)], list(result['Two']))

    def test_time_series_date(self):
        # Tests transformation of a single-metric, single-dimension result
        result = self.pd_tx.transform(self.time_dim_single_metric_df, self.time_dim_single_metric_schema)

        self.assertListEqual(['Date'], list(result.index.names))
        self.assertListEqual([datetime(2000, 1, i) for i in range(1, 9)], list(result.index))

        self.assertListEqual(['One'], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result['One']))

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.pd_tx.transform(self.time_dim_single_metric_ref_df, self.time_dim_single_metric_ref_schema)

        self.assertListEqual(['Date'], list(result.index.names))
        self.assertListEqual([datetime(2000, 1, i) for i in range(1, 9)], list(result.index))

        self.assertListEqual([('One', None), ('One', 'WoW')], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result[('One', None)]))
        self.assertListEqual([2 * a for a in range(8)], list(result[('One', 'WoW')]))

    def test_uni_dim_single_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and display
        result = self.pd_tx.transform(self.uni_dim_single_metric_df, self.uni_dim_single_metric_schema)

        self.assertListEqual(['Uni'], list(result.index.names))
        self.assertListEqual(['One'], list(result.columns))

    def test_uni_dim_multi_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and display
        result = self.pd_tx.transform(self.uni_dim_multi_metric_df, self.uni_dim_multi_metric_schema)

        self.assertListEqual(['Uni'], list(result.index.names))
        self.assertListEqual(['Aa', 'Bb', 'Cc'], list(result.index))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(3)], list(result['One']))
        self.assertListEqual([2 * a for a in range(3)], list(result['Two']))

    def test_cat_cat_dim_single_metric(self):
        # Tests transformation of a single metric with two categorical dimensions
        result = self.pd_tx.transform(self.cat_cat_dims_single_metric_df, self.cat_cat_dims_single_metric_schema)

        self.assertListEqual(['Cat1', 'Cat2'], list(result.index.names))
        self.assertListEqual(['A', 'B'], list(result.index.levels[0]))
        self.assertListEqual(['Y', 'Z'], list(result.index.levels[1]))

        self.assertListEqual(['One'], list(result.columns))
        self.assertListEqual([a for a in range(4)], list(result['One']))

    def test_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        result = self.pd_tx.transform(self.cat_cat_dims_multi_metric_df, self.cat_cat_dims_multi_metric_schema)

        self.assertListEqual(['Cat1', 'Cat2'], list(result.index.names))
        self.assertListEqual(['A', 'B'], list(result.index.levels[0]))
        self.assertListEqual(['Y', 'Z'], list(result.index.levels[1]))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(4)], list(result['One']))
        self.assertListEqual([2 * a for a in range(4)], list(result['Two']))

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.pd_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.assertListEqual(['Cont', 'Cat1', 'Cat2'], list(result.index.names))
        self.assertListEqual([0, 1, 2, 3, 4, 5, 6, 7], list(result.index.levels[0]))
        self.assertListEqual([np.nan, 'A', 'B'], list(result.index.levels[1]))
        self.assertListEqual([np.nan, 'Y', 'Z'], list(result.index.levels[2]))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([12, 1, 0, 1, 5, 2, 3,
                              44, 9, 4, 5, 13, 6, 7,
                              76, 17, 8, 9, 21, 10, 11,
                              108, 25, 12, 13, 29, 14, 15,
                              140, 33, 16, 17, 37, 18, 19,
                              172, 41, 20, 21, 45, 22, 23,
                              204, 49, 24, 25, 53, 26, 27,
                              236, 57, 28, 29, 61, 30, 31], list(result['One']))
        self.assertListEqual([24, 2, 0, 2, 10, 4, 6,
                              88, 18, 8, 10, 26, 12, 14,
                              152, 34, 16, 18, 42, 20, 22,
                              216, 50, 24, 26, 58, 28, 30,
                              280, 66, 32, 34, 74, 36, 38,
                              344, 82, 40, 42, 90, 44, 46,
                              408, 98, 48, 50, 106, 52, 54,
                              472, 114, 56, 58, 122, 60, 62], list(result['Two']))


class PandasColumnIndexTransformerTests(BaseTransformerTests):
    pd_tx = PandasRowIndexTransformer()

    def test_no_dims_multi_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.pd_tx.transform(self.no_dims_multi_metric_df, self.no_dims_multi_metric_schema)

        self.assertListEqual([None], list(result.index.names))
        self.assertListEqual([0], list(result.index))

        self.assertListEqual(['One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight'], list(result.columns))
        self.assertListEqual([a for a in range(8)], result.values[0].tolist())

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.pd_tx.transform(self.cont_dim_single_metric_df, self.cont_dim_single_metric_schema)

        self.assertListEqual(['Cont'], list(result.index.names))
        self.assertListEqual([a for a in range(8)], list(result.index))

        self.assertListEqual(['One'], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result['One']))

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        result = self.pd_tx.transform(self.cont_dim_multi_metric_df, self.cont_dim_multi_metric_schema)

        self.assertListEqual(['Cont'], list(result.index.names))
        self.assertListEqual([a for a in range(8)], list(result.index))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result['One']))
        self.assertListEqual([2 * a for a in range(8)], list(result['Two']))

    def test_time_series_date_to_millis(self):
        # Tests transformation of a single-metric, single-dimension result
        result = self.pd_tx.transform(self.time_dim_single_metric_df, self.time_dim_single_metric_schema)

        self.assertListEqual(['Date'], list(result.index.names))
        self.assertListEqual([datetime(2000, 1, i) for i in range(1, 9)], list(result.index))

        self.assertListEqual(['One'], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result['One']))

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.pd_tx.transform(self.time_dim_single_metric_ref_df, self.time_dim_single_metric_ref_schema)

        self.assertListEqual(['Date'], list(result.index.names))
        self.assertListEqual([datetime(2000, 1, i) for i in range(1, 9)], list(result.index))

        self.assertListEqual([('One', None), ('One', 'WoW')], list(result.columns))
        self.assertListEqual([a for a in range(8)], list(result[('One', None)]))
        self.assertListEqual([2 * a for a in range(8)], list(result[('One', 'WoW')]))

    def test_cont_cat_dim_single_metric(self):
        # Tests transformation of a single metric with a continuous and a categorical dimension
        result = self.pd_tx.transform(self.cont_cat_dims_single_metric_df, self.cont_cat_dims_single_metric_schema)

        self.assertListEqual(['Cont', 'Cat1'], list(result.index.names))
        self.assertListEqual([(a, b)
                              for a in range(8)
                              for b in ['A', 'B']], list(result.index))

        self.assertListEqual(['One'], list(result.columns))
        self.assertListEqual([a for a in range(16)], list(result['One']))

    def test_cont_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and a categorical dimension
        result = self.pd_tx.transform(self.cont_cat_dims_multi_metric_df, self.cont_cat_dims_multi_metric_schema)

        self.assertListEqual(['Cont', 'Cat1'], list(result.index.names))
        self.assertListEqual([(a, b)
                              for a in range(8)
                              for b in ['A', 'B']], list(result.index))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(16)], list(result['One']))
        self.assertListEqual([2 * a for a in range(16)], list(result['Two']))

    def test_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.pd_tx.transform(self.cont_cat_cat_dims_multi_metric_df,
                                      self.cont_cat_cat_dims_multi_metric_schema)

        self.assertListEqual(['Cont', 'Cat1', 'Cat2'], list(result.index.names))
        self.assertListEqual([(a, b, c)
                              for a in range(8)
                              for b in ['A', 'B']
                              for c in ['Y', 'Z']], list(result.index))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(32)], list(result['One']))
        self.assertListEqual([2 * a for a in range(32)], list(result['Two']))

    def test_cont_cat_uni_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.pd_tx.transform(self.cont_cat_uni_dims_multi_metric_df,
                                      self.cont_cat_uni_dims_multi_metric_schema)

        self.assertListEqual(['Cont', 'Cat1', 'Uni'], list(result.index.names))
        self.assertListEqual([(a, b, c)
                              for a in range(8)
                              for b in ['A', 'B']
                              for c in ['Aa', 'Bb', 'Cc']], list(result.index))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([a for a in range(48)], list(result['One']))
        self.assertListEqual([a for a in range(100, 148)], list(result['Two']))

    def test_rollup_cont_cat_cat_dims_multi_metric_df(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.pd_tx.transform(self.rollup_cont_cat_cat_dims_multi_metric_df,
                                      self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.assertListEqual(['Cont', 'Cat1', 'Cat2'], list(result.index.names))
        self.assertListEqual([0, 1, 2, 3, 4, 5, 6, 7], list(result.index.levels[0]))
        self.assertListEqual([np.nan, 'A', 'B'], list(result.index.levels[1]))
        self.assertListEqual([np.nan, 'Y', 'Z'], list(result.index.levels[2]))

        self.assertListEqual(['One', 'Two'], list(result.columns))
        self.assertListEqual([12, 1, 0, 1, 5, 2, 3,
                              44, 9, 4, 5, 13, 6, 7,
                              76, 17, 8, 9, 21, 10, 11,
                              108, 25, 12, 13, 29, 14, 15,
                              140, 33, 16, 17, 37, 18, 19,
                              172, 41, 20, 21, 45, 22, 23,
                              204, 49, 24, 25, 53, 26, 27,
                              236, 57, 28, 29, 61, 30, 31], list(result['One']))
        self.assertListEqual([24, 2, 0, 2, 10, 4, 6,
                              88, 18, 8, 10, 26, 12, 14,
                              152, 34, 16, 18, 42, 20, 22,
                              216, 50, 24, 26, 58, 28, 30,
                              280, 66, 32, 34, 74, 36, 38,
                              344, 82, 40, 42, 90, 44, 46,
                              408, 98, 48, 50, 106, 52, 54,
                              472, 114, 56, 58, 122, 60, 62], list(result['Two']))


@patch('matplotlib.pyplot')
class MatplotlibLineChartTransformerTests(BaseTransformerTests):
    from fireant.slicer.transformers import MatplotlibLineChartTransformer

    plt_tx = MatplotlibLineChartTransformer()

    def _assert_matplotlib_calls(self, mock_plot, metrics):
        if 1 == len(metrics):
            mock_plot.line.assert_has_calls([call(figsize=(14, 5)),
                                             call().legend(loc='center left', bbox_to_anchor=(1, 0.5)),
                                             call().legend().set_title(metrics[0])])
            return

        mock_plot.line.assert_has_calls([c
                                         for metric in metrics
                                         for c in [call(ax=ANY),
                                                   call().legend(loc='center left', bbox_to_anchor=(1, 0.5)),
                                                   call().legend().set_title(metric)]])

    @patch('pandas.DataFrame.plot')
    def test_series_single_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a single-metric, single-dimension result
        result = self.plt_tx.transform(self.cont_dim_single_metric_df, self.cont_dim_single_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One'])

    @patch('pandas.Series.plot')
    def test_series_multi_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a multi-metric, single-dimension result
        result = self.plt_tx.transform(self.cont_dim_multi_metric_df, self.cont_dim_multi_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One', 'Two'])

    @patch('pandas.DataFrame.plot')
    def test_time_series_date_to_millis(self, mock_plot, mock_matplotlib):
        # Tests transformation of a single-metric, single-dimension result
        result = self.plt_tx.transform(self.time_dim_single_metric_df, self.time_dim_single_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One'])

    @patch('pandas.DataFrame.plot')
    def test_time_series_date_with_ref(self, mock_plot, mock_matplotlib):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.plt_tx.transform(self.time_dim_single_metric_ref_df, self.time_dim_single_metric_ref_schema)

        self._assert_matplotlib_calls(mock_plot, ['One'])

    @patch('pandas.DataFrame.plot')
    def test_cont_uni_dim_single_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a metric and a unique dimension
        result = self.plt_tx.transform(self.cont_uni_dims_single_metric_df, self.cont_uni_dims_single_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One'])

    @patch('pandas.DataFrame.plot')
    def test_cont_uni_dim_multi_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of two metrics and a unique dimension
        result = self.plt_tx.transform(self.cont_uni_dims_multi_metric_df, self.cont_uni_dims_multi_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One', 'Two'])

    @patch('pandas.DataFrame.plot')
    def test_double_dimension_single_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a single-metric, double-dimension result
        result = self.plt_tx.transform(self.cont_cat_dims_single_metric_df, self.cont_cat_dims_single_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One'])

    @patch('pandas.DataFrame.plot')
    def test_double_dimension_multi_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a multi-metric, double-dimension result
        result = self.plt_tx.transform(self.cont_cat_dims_multi_metric_df, self.cont_cat_dims_multi_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One', 'Two'])

    @patch('pandas.DataFrame.plot')
    def test_triple_dimension_multi_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a multi-metric, double-dimension result
        df = self.cont_cat_cat_dims_multi_metric_df

        result = self.plt_tx.transform(df, self.cont_cat_cat_dims_multi_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One', 'Two'])

    @patch('pandas.DataFrame.plot')
    def test_rollup_triple_dimension_multi_metric(self, mock_plot, mock_matplotlib):
        # Tests transformation of a multi-metric, double-dimension result
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.plt_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self._assert_matplotlib_calls(mock_plot, ['One', 'Two'])

    def test_require_at_least_one_dimension(self, mock_plot):
        df = self.no_dims_multi_metric_df

        with self.assertRaises(TransformationException):
            self.plt_tx.transform(df, self.no_dims_multi_metric_schema)
