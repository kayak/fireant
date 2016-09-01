# coding: utf-8

from fireant.slicer.postprocessors import OperationManager
from slicer.transformers.base import BaseTransformerTests


class PostProcessingTests(BaseTransformerTests):
    manager = OperationManager()
    maxDiff = None


class LossOperationTests(PostProcessingTests):
    pass


class CumulativeOperationTests(PostProcessingTests):
    def test_cumsum_single_dim(self):
        df = self.time_dim_single_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metrics': ['one']}])

        # original DF unchanged
        self.assertListEqual(['one'], list(df.columns))

        self.assertListEqual(['one', 'one_cumsum'], list(result_df.columns))
        self.assertListEqual(list(df['one'].cumsum()), list(result_df['one_cumsum']))

    def test_cumsum_single_dim_extra_metrics(self):
        df = self.cont_dim_multi_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metrics': ['one']}])

        # original DF unchanged
        self.assertListEqual(['one', 'two'], list(df.columns))

        self.assertListEqual(['one', 'two', 'one_cumsum'], list(result_df.columns))
        self.assertListEqual(list(df['one'].cumsum()), list(result_df['one_cumsum']))
        self.assertListEqual(list(df['two']), list(result_df['two']))

    def test_cumsum_single_dim_both_metrics(self):
        df = self.cont_dim_multi_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metrics': ['one', 'two']}])
        # original DF unchanged
        self.assertListEqual(['one', 'two'], list(df.columns))

        self.assertListEqual(['one', 'two', 'one_cumsum', 'two_cumsum'], list(result_df.columns))
        self.assertListEqual(list(df['one'].cumsum()), list(result_df['one_cumsum']))
        self.assertListEqual(list(df['two'].cumsum()), list(result_df['two_cumsum']))

    def test_cumsum_single_dim_with_ref(self):
        df = self.time_dim_single_metric_ref_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metrics': ['one']}])

        # original DF unchanged
        self.assertListEqual([('', 'one'), ('wow', 'one')], list(df.columns))

        self.assertListEqual([('', 'one'), ('wow', 'one'), ('', 'one_cumsum'), ('wow', 'one_cumsum')],
                             list(result_df.columns))
        self.assertListEqual(list(df[('', 'one')].cumsum()),
                             list(result_df[('', 'one_cumsum')]))
        self.assertListEqual(list(df[('wow', 'one')].cumsum()),
                             list(result_df[('wow', 'one_cumsum')]))

    def test_cumsum_multi_dim(self):
        df = self.cont_cat_dims_single_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metrics': ['one']}])

        self.assertListEqual(['one', 'one_cumsum'], list(result_df.columns))
        self.assertListEqual(list(df.loc[(slice(None), 'a'), 'one'].cumsum()),
                             list(result_df.loc[(slice(None), 'a'), 'one_cumsum']))
        self.assertListEqual(list(df.loc[(slice(None), 'b'), 'one'].cumsum()),
                             list(result_df.loc[(slice(None), 'b'), 'one_cumsum']))

    def test_cummean_single_dim(self):
        result_df = self.manager.post_process(self.time_dim_single_metric_df, [{'key': 'cummean',
                                                                                'metrics': ['one']}])

        self.assertListEqual(['one', 'one_cummean'], list(result_df.columns))
        self.assertListEqual(list(self.time_dim_single_metric_df['one'].expanding(min_periods=1).mean()),
                             list(result_df['one_cummean']))

    def test_cummean_single_dim_with_ref(self):
        df = self.time_dim_single_metric_ref_df
        result_df = self.manager.post_process(df, [{'key': 'cummean',
                                                    'metrics': ['one']}])

        self.assertListEqual([('', 'one'), ('wow', 'one'), ('', 'one_cummean'), ('wow', 'one_cummean')],
                             list(result_df.columns))
        self.assertListEqual(list(df[('', 'one')].expanding(min_periods=1).mean()),
                             list(result_df[('', 'one_cummean')]))
        self.assertListEqual(list(df[('wow', 'one')].expanding(min_periods=1).mean()),
                             list(result_df[('wow', 'one_cummean')]))

    def test_cummean_multi_dim(self):
        df = self.cont_cat_dims_single_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cummean',
                                                    'metrics': ['one']}])

        self.assertListEqual(['one', 'one_cummean'], list(result_df.columns))
        self.assertListEqual(list(df.loc[(slice(None), 'a'), 'one'].expanding(min_periods=1).mean()),
                             list(result_df.loc[(slice(None), 'a'), 'one_cummean']))
        self.assertListEqual(list(df.loc[(slice(None), 'b'), 'one'].expanding(min_periods=1).mean()),
                             list(result_df.loc[(slice(None), 'b'), 'one_cummean']))
