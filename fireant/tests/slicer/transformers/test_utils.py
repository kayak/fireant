# coding: utf-8

from fireant.slicer.transformers import utils
from fireant.tests.slicer.transformers.base import BaseTransformerTests


class UtilsTest(BaseTransformerTests):
    def test_correct_dims_cont_cat(self):
        df = self.cont_cat_dims_multi_metric_df.reorder_levels([1, 0])

        result = utils.correct_dimension_level_order(df, self.cont_cat_dims_multi_metric_schema)

        self.assertListEqual(['cont', 'cat1'], list(result.index.names))

    def test_correct_dims_cont_cat_uni(self):
        df = self.cont_cat_uni_dims_multi_metric_df.reorder_levels([2, 0, 3, 1])

        result = utils.correct_dimension_level_order(df, self.cont_cat_uni_dims_multi_metric_schema)

        self.assertListEqual(['cont', 'cat1', 'uni', 'uni_label'], list(result.index.names))
