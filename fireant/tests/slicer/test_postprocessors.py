# coding: utf-8
from unittest import TestCase

import numpy as np

from fireant.slicer.postprocessors import OperationManager
from fireant.tests import mock_dataframes as mock_df


class PostProcessingTests(object):
    manager = OperationManager()
    maxDiff = None


class CumulativeSumOperationTests(PostProcessingTests, TestCase):
    @property
    def op_key(self):
        return 'cumsum'

    def operation(self, df):
        return list(df.expanding(min_periods=1).sum())

    def test_single_dim(self):
        df = mock_df.time_dim_single_metric_df
        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one'}])

        # original DF unchanged
        self.assertListEqual(['one'], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual(['one', operation_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['one']), list(result_df[operation_key]))

    def test_single_dim_extra_metrics(self):
        df = mock_df.cont_dim_multi_metric_df
        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one'}])

        # original DF unchanged
        self.assertListEqual(['one', 'two'], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual(['one', 'two', operation_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['one']), list(result_df[operation_key]))
        np.testing.assert_array_almost_equal(list(df['two']), list(result_df['two']))

    def test_single_dim_both_metrics(self):
        df = mock_df.cont_dim_multi_metric_df
        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one'},
                                                   {'key': self.op_key, 'metric': 'two'}])
        # original DF unchanged
        self.assertListEqual(['one', 'two'], list(df.columns))

        operation1_key = 'one_%s' % self.op_key
        operation2_key = 'two_%s' % self.op_key
        self.assertListEqual(['one', 'two', operation1_key, operation2_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['one']), list(result_df[operation1_key]))
        np.testing.assert_array_almost_equal(self.operation(df['two']), list(result_df[operation2_key]))

    def test_single_dim_with_ref(self):
        df = mock_df.time_dim_single_metric_ref_df
        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one'}])

        # original DF unchanged
        self.assertListEqual([('', 'one'), ('wow', 'one')], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual([('', 'one'), ('wow', 'one'), ('', operation_key), ('wow', operation_key)],
                             list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['', 'one']),
                                             list(result_df[('', operation_key)]))
        np.testing.assert_array_almost_equal(self.operation(df['wow', 'one']),
                                             list(result_df[('wow', operation_key)]))

    def test_multi_dim(self):
        df = mock_df.cont_cat_dims_single_metric_df
        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one'}])

        # original DF unchanged
        self.assertListEqual(['one'], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual(['one', operation_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df.loc[(slice(None), 'a'), 'one']),
                                             list(result_df.loc[(slice(None), 'a'), operation_key]))
        np.testing.assert_array_almost_equal(self.operation(df.loc[(slice(None), 'b'), 'one']),
                                             list(result_df.loc[(slice(None), 'b'), operation_key]))


class CumulativeMeanOperationTests(CumulativeSumOperationTests, TestCase):
    @property
    def op_key(self):
        return 'cummean'

    def operation(self, df):
        return list(df.expanding(min_periods=1).mean())


class L1LossOperationTests(PostProcessingTests, TestCase):
    @property
    def op_key(self):
        return 'l1loss'

    def operation(self, metric_df, target_df):
        return list((metric_df - target_df).abs().expanding(min_periods=1).mean())

    def test_single_dim(self):
        df = mock_df.time_dim_single_metric_df.copy()
        df['target'] = [2, 0, 4, 0, 6, 0, 8, 0]

        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one', 'target': 'target'}])

        # original DF unchanged
        self.assertListEqual(['one', 'target'], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual(['one', 'target', operation_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['one'], df['target']), list(result_df[operation_key]))

    def test_single_dim_extra_metrics(self):
        df = mock_df.cont_dim_multi_metric_df.copy()
        df['target'] = [2, 0, 4, 0, 6, 0, 8, 0]

        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one', 'target': 'target'}])

        # original DF unchanged
        self.assertListEqual(['one', 'two', 'target'], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual(['one', 'two', 'target', operation_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['one'], df['target']), list(result_df[operation_key]))
        np.testing.assert_array_almost_equal(list(df['two']), list(result_df['two']))

    def test_single_dim_both_metrics(self):
        df = mock_df.cont_dim_multi_metric_df.copy()
        df['target'] = [2, 0, 4, 0, 6, 0, 8, 0]

        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one', 'target': 'target'},
                                                   {'key': self.op_key, 'metric': 'two', 'target': 'target'}])
        # original DF unchanged
        self.assertListEqual(['one', 'two', 'target'], list(df.columns))

        operation1_key = 'one_%s' % self.op_key
        operation2_key = 'two_%s' % self.op_key
        self.assertListEqual(['one', 'two', 'target', operation1_key, operation2_key], list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['one'], df['target']), list(result_df[operation1_key]))
        np.testing.assert_array_almost_equal(self.operation(df['two'], df['target']), list(result_df[operation2_key]))

    def test_single_dim_with_ref(self):
        df = mock_df.time_dim_single_metric_ref_df.copy()
        df['', 'target'] = [2, 0, 4, 0, 6, 0, 8, 0]
        df['wow', 'target'] = [2, 0, 4, 0, 6, 0, 8, 0]

        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one', 'target': 'target'}])

        # original DF unchanged
        self.assertListEqual([('', 'one'), ('wow', 'one'), ('', 'target'), ('wow', 'target')], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual([('', 'one'), ('wow', 'one'),
                              ('', 'target'), ('wow', 'target'),
                              ('', operation_key), ('wow', operation_key)],
                             list(result_df.columns))
        np.testing.assert_array_almost_equal(self.operation(df['', 'one'], df['', 'target']),
                                             list(result_df[('', operation_key)]))
        np.testing.assert_array_almost_equal(self.operation(df['wow', 'one'], df['wow', 'target']),
                                             list(result_df[('wow', operation_key)]))

    def test_multi_dim(self):
        df = mock_df.cont_cat_dims_single_metric_df.copy()
        df['target'] = df['one'] + 1

        result_df = self.manager.post_process(df, [{'key': self.op_key, 'metric': 'one', 'target': 'target'}])

        # original DF unchanged
        self.assertListEqual(['one', 'target'], list(df.columns))

        operation_key = 'one_%s' % self.op_key
        self.assertListEqual(['one', 'target', operation_key], list(result_df.columns))

        slice_a = df.loc[(slice(None), 'a'), :]
        np.testing.assert_array_almost_equal(self.operation(slice_a['one'], slice_a['target']),
                                             list(result_df.loc[(slice(None), 'a'), operation_key]))

        slice_b = df.loc[(slice(None), 'b'), :]
        np.testing.assert_array_almost_equal(self.operation(slice_b['one'], slice_b['target']),
                                             list(result_df.loc[(slice(None), 'b'), operation_key]))


class L2LossOperationTests(L1LossOperationTests, TestCase):
    @property
    def op_key(self):
        return 'l2loss'

    def operation(self, metric_df, target_df):
        return list((metric_df - target_df).pow(2).expanding(min_periods=1).mean())
