from unittest import TestCase

import pandas as pd
import pandas.testing

from fireant import (
    CumMean,
    CumProd,
    CumSum,
)
from fireant.tests.dataset.mocks import (
    ElectionOverElection,
    dimx1_date_df,
    dimx2_date_str_df,
    dimx2_date_str_ref_df,
    mock_dataset,
)


class CumSumTests(TestCase):
    def test_apply_to_timeseries(self):
        cumsum = CumSum(mock_dataset.fields.wins)
        result = cumsum.apply(dimx1_date_df, None)

        expected = pd.Series([2, 4, 6, 8, 10, 12],
                             name='$wins',
                             index=dimx1_date_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim(self):
        cumsum = CumSum(mock_dataset.fields.wins)
        result = cumsum.apply(dimx2_date_str_df, None)

        expected = pd.Series([2, 0, 0, 2, 2, 2, 4, 4, 4, 6, 4, 6, 6],
                             name='$wins',
                             index=dimx2_date_str_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        cumsum = CumSum(mock_dataset.fields.wins)
        result = cumsum.apply(dimx2_date_str_ref_df, ElectionOverElection(mock_dataset.fields.timestamp))

        expected = pd.Series([2.0, 0.0, 2.0, 0.0, 4.0, 0.0, 6.0, 2.0, 6.0, 4.0, 6.0],
                             name='$wins_eoe',
                             index=dimx2_date_str_ref_df.index)
        pandas.testing.assert_series_equal(expected, result)


class CumProdTests(TestCase):
    def test_apply_to_timeseries(self):
        cumprod = CumProd(mock_dataset.fields.wins)
        result = cumprod.apply(dimx1_date_df, None)

        expected = pd.Series([2, 4, 8, 16, 32, 64],
                             name='$wins',
                             index=dimx1_date_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim(self):
        cumprod = CumProd(mock_dataset.fields.wins)
        result = cumprod.apply(dimx2_date_str_df, None)

        expected = pd.Series([2] + [0] * 12,
                             name='$wins',
                             index=dimx2_date_str_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        cumprod = CumProd(mock_dataset.fields.wins)
        result = cumprod.apply(dimx2_date_str_ref_df, ElectionOverElection(mock_dataset.fields.timestamp))

        expected = pd.Series([2.] + [0.] * 10,
                             name='$wins_eoe',
                             index=dimx2_date_str_ref_df.index)
        pandas.testing.assert_series_equal(expected, result)


class CumMeanTests(TestCase):
    def test_apply_to_timeseries(self):
        cummean = CumMean(mock_dataset.fields.votes)
        result = cummean.apply(dimx1_date_df, None)

        expected = dimx1_date_df['$votes'].astype(float).cumsum() / range(1, len(dimx1_date_df) + 1)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim(self):
        cummean = CumMean(mock_dataset.fields.votes)
        result = cummean.apply(dimx2_date_str_df, None)

        expected = pd.Series([7579518.0, 1076384.0, 6564547.0, 7937233.5, 7465807.5, 8484218.666666666, 8322786.0,
                              9313940.5, 8614866.75, 9935978.0, 8521509.8, 9091928.0, 9341064.0],
                             name='$votes',
                             index=dimx2_date_str_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        cummean = CumMean(mock_dataset.fields.votes)
        result = cummean.apply(dimx2_date_str_ref_df, ElectionOverElection(mock_dataset.fields.timestamp))

        expected = pd.Series([7579518.0, 1076384.0, 7072032.5, 4685666.5, 7503711.0, 6316507.333333333, 8136969.0,
                              7688157.0, 8407797.0, 8635351.2, 8364511.166666667],
                             name='$votes_eoe',
                             index=dimx2_date_str_ref_df.index)
        pandas.testing.assert_series_equal(expected, result)
