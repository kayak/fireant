# coding: utf-8
from unittest import TestCase

from fireant import utils
from fireant.tests import mock_dataframes as mock_df


class UtilsTest(TestCase):
    def test_correct_dims_cont_cat(self):
        df = mock_df.cont_cat_dims_multi_metric_df.reorder_levels([1, 0])

        result = utils.correct_dimension_level_order(df, mock_df.cont_cat_dims_multi_metric_schema)

        self.assertListEqual(['cont', 'cat1'], list(result.index.names))

    def test_correct_dims_cont_cat_uni(self):
        df = mock_df.cont_cat_uni_dims_multi_metric_df.reorder_levels([2, 0, 3, 1])

        result = utils.correct_dimension_level_order(df, mock_df.cont_cat_uni_dims_multi_metric_schema)

        self.assertListEqual(['cont', 'cat1', 'uni', 'uni_label'], list(result.index.names))

    def test_correct_metrics(self):
        df = mock_df.cont_cat_uni_dims_multi_metric_df

        result = utils.correct_dimension_level_order(df[['two', 'one']], mock_df.cont_cat_uni_dims_multi_metric_schema)

        self.assertListEqual(['one', 'two'], list(result.columns))

    def test_correct_metrics_ref(self):
        df = mock_df.time_dim_single_metric_ref_df

        result = utils.correct_dimension_level_order(df[[('wow', 'one'), ('', 'one')]],
                                                     mock_df.time_dim_single_metric_ref_schema)

        self.assertListEqual([('', 'one'), ('wow', 'one')], list(result.columns))
