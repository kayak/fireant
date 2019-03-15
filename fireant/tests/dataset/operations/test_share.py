from unittest import TestCase

import pandas as pd
import pandas.testing

from fireant import Share
from fireant.tests.dataset.mocks import (
    dimx0_metricx1_df,
    dimx1_str_df,
    dimx1_str_totals_df,
    dimx2_date_str_df,
    dimx2_date_str_totals_df,
    dimx2_date_str_totalsx2_df,
    mock_dataset,
)
from fireant.utils import alias_selector


class ShareTests(TestCase):
    def test_apply_to_zero_dims(self):
        share = Share(mock_dataset.fields.votes)
        result = share.apply(dimx0_metricx1_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([100.],
                             name=f_metric_key)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_one_dim_over_first(self):
        share = Share(mock_dataset.fields.votes, over=mock_dataset.fields.political_party)
        result = share.apply(dimx1_str_totals_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([48.8487, 0.9638, 50.1873, 100.0],
                             name=f_metric_key,
                             index=dimx1_str_totals_df.index)
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_one_dim_over_none(self):
        share = Share(mock_dataset.fields.votes)
        result = share.apply(dimx1_str_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([100.] * 3,
                             name=f_metric_key,
                             index=dimx1_str_df.index)
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_first(self):
        share = Share(mock_dataset.fields.votes, over=mock_dataset.fields.timestamp)
        result = share.apply(dimx2_date_str_totalsx2_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        metric_series = dimx2_date_str_totalsx2_df[f_metric_key]
        expected = 100 * metric_series / metric_series.iloc[-1]
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_second(self):
        share = Share(mock_dataset.fields.votes, over=mock_dataset.fields.political_party)
        result = share.apply(dimx2_date_str_totals_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([49.79, 7.07, 43.12, 100.0,
                              49.78, 50.21, 100.0,
                              48.83, 51.16, 100.0,
                              55.42, 44.57, 100.0,
                              60.39, 39.60, 100.0,
                              26.60, 73.39, 100.0],
                             name=f_metric_key,
                             index=dimx2_date_str_totals_df.index)

        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_second_all_totals(self):
        share = Share(mock_dataset.fields.votes, over=mock_dataset.fields.political_party)
        result = share.apply(dimx2_date_str_totalsx2_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([49.79, 7.07, 43.12, 100.0,
                              49.78, 50.21, 100.0,
                              48.83, 51.16, 100.0,
                              55.42, 44.57, 100.0,
                              60.39, 39.60, 100.0,
                              26.60, 73.39, 100.0, 100.0],
                             name=f_metric_key,
                             index=dimx2_date_str_totalsx2_df.index)

        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_second_with_one_row_per_group(self):
        raw_df = dimx2_date_str_totals_df.iloc[[0, 3, 4, 6]]

        share = Share(mock_dataset.fields.votes, over=mock_dataset.fields.political_party)
        result = share.apply(raw_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([49.79, 100.0, 49.78, 100.0],
                             name=f_metric_key,
                             index=raw_df.index)

        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)

    def test_apply_to_two_dims_over_none(self):
        share = Share(mock_dataset.fields.votes)
        result = share.apply(dimx2_date_str_df, None)

        f_metric_key = alias_selector(mock_dataset.fields.votes.alias)

        expected = pd.Series([100.] * 13,
                             name=f_metric_key,
                             index=dimx2_date_str_df.index)
        pandas.testing.assert_series_equal(expected, result, check_less_precise=True)
