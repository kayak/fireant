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
                                                    'metric': 'one'}])

        self.assertListEqual(list(df.columns), list(result_df.columns))
        self.assertListEqual(list(df['one'].cumsum()), list(result_df['one']))

    def test_cumsum_single_dim_extra_metrics(self):
        df = self.cont_dim_multi_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metric': 'one'}])

        self.assertListEqual(list(df.columns), list(result_df.columns))
        self.assertListEqual(list(df['one'].cumsum()), list(result_df['one']))
        self.assertListEqual(list(df['two']), list(result_df['two']))

    def test_cumsum_single_dim_with_ref(self):
        df = self.time_dim_single_metric_ref_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metric': 'one'}])

        self.assertListEqual(list(df.columns), list(result_df.columns))
        self.assertListEqual(list(df[('', 'one')].cumsum()),
                             list(result_df[('', 'one')]))
        self.assertListEqual(list(df[('wow', 'one')].cumsum()),
                             list(result_df[('wow', 'one')]))

    def test_cumsum_multi_dim(self):
        df = self.cont_cat_dims_single_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cumsum',
                                                    'metric': 'one'}])

        self.assertListEqual(list(df.columns), list(result_df.columns))
        self.assertListEqual(list(df.loc[(slice(None), 'a'), 'one'].cumsum()),
                             list(result_df.loc[(slice(None), 'a'), 'one']))
        self.assertListEqual(list(df.loc[(slice(None), 'b'), 'one'].cumsum()),
                             list(result_df.loc[(slice(None), 'b'), 'one']))

    def test_cummean_single_dim(self):
        result_df = self.manager.post_process(self.time_dim_single_metric_df, [{'key': 'cummean',
                                                                                'metric': 'one'}])

        self.assertListEqual(list(self.time_dim_single_metric_df['one'].expanding(min_periods=1).mean()),
                             list(result_df['one']))

    def test_cummean_single_dim_with_ref(self):
        df = self.time_dim_single_metric_ref_df
        result_df = self.manager.post_process(df, [{'key': 'cummean',
                                                    'metric': 'one'}])

        self.assertListEqual(list(df.columns), list(result_df.columns))
        self.assertListEqual(list(df[('', 'one')].expanding(min_periods=1).mean()),
                             list(result_df[('', 'one')]))
        self.assertListEqual(list(df[('wow', 'one')].expanding(min_periods=1).mean()),
                             list(result_df[('wow', 'one')]))

    def test_cummean_multi_dim(self):
        df = self.cont_cat_dims_single_metric_df
        result_df = self.manager.post_process(df, [{'key': 'cummean',
                                                    'metric': 'one'}])

        self.assertListEqual(list(df.columns), list(result_df.columns))
        self.assertListEqual(
            list(df.loc[(slice(None), 'a'), 'one'].expanding(min_periods=1).mean()),
            list(result_df.loc[(slice(None), 'a'), 'one']))
        self.assertListEqual(
            list(df.loc[(slice(None), 'b'), 'one'].expanding(min_periods=1).mean()),
            list(result_df.loc[(slice(None), 'b'), 'one']))
