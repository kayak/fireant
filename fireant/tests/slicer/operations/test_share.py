from unittest import TestCase

import pandas as pd
import pandas.testing

from fireant import Share
from fireant.tests.slicer.mocks import (
    cat_dim_df,
    cat_dim_totals_df,
    cont_uni_dim_all_totals_df,
    cont_uni_dim_df,
    cont_uni_dim_totals_df,
    single_metric_df,
    slicer,
)
from fireant.utils import format_metric_key


class ShareTests(TestCase):
    def test_apply_to_zero_dims(self):
        share = Share(slicer.metrics.votes)
        result = share.apply(single_metric_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([100.],
                             name=f_metric_key)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_one_dim_over_first(self):
        share = Share(slicer.metrics.votes, over=slicer.dimensions.political_party)
        result = share.apply(cat_dim_totals_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([48.849, 0.964, 50.187, 100.0],
                             name=f_metric_key,
                             index=cat_dim_totals_df.index)
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_one_dim_over_none(self):
        share = Share(slicer.metrics.votes)
        result = share.apply(cat_dim_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([100.] * 3,
                             name=f_metric_key,
                             index=cat_dim_df.index)
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_first(self):
        share = Share(slicer.metrics.votes, over=slicer.dimensions.timestamp)
        result = share.apply(cont_uni_dim_all_totals_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        metric_series = cont_uni_dim_all_totals_df[f_metric_key]
        expected = 100 * metric_series / metric_series.iloc[-1]
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_second(self):
        share = Share(slicer.metrics.votes, over=slicer.dimensions.state)
        result = share.apply(cont_uni_dim_totals_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([36.624, 63.376, 100.,
                              37.411, 62.589, 100.,
                              37.521, 62.479, 100.,
                              37.606, 62.394, 100.,
                              38.294, 61.706, 100.,
                              27.705, 72.295, 100.],
                             name=f_metric_key,
                             index=cont_uni_dim_totals_df.index)

        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_second_all_totals(self):
        share = Share(slicer.metrics.votes, over=slicer.dimensions.state)
        result = share.apply(cont_uni_dim_all_totals_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([36.624, 63.376, 100.,
                              37.411, 62.589, 100.,
                              37.521, 62.479, 100.,
                              37.606, 62.394, 100.,
                              38.294, 61.706, 100.,
                              27.705, 72.295, 100.,
                              100.],
                             name=f_metric_key,
                             index=cont_uni_dim_all_totals_df.index)

        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_second_with_one_row_per_group(self):
        raw_df = cont_uni_dim_totals_df.iloc[[0, 2, 3, 5]]

        share = Share(slicer.metrics.votes, over=slicer.dimensions.state)
        result = share.apply(raw_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([36.624, 100., 37.411, 100.],
                             name=f_metric_key,
                             index=raw_df.index)

        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_none(self):
        share = Share(slicer.metrics.votes)
        result = share.apply(cont_uni_dim_df, None)

        f_metric_key = format_metric_key(slicer.metrics.votes.key)

        expected = pd.Series([100.] * 12,
                             name=f_metric_key,
                             index=cont_uni_dim_df.index)
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)
