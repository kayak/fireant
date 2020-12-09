from unittest import TestCase

import pandas as pd
import pandas.testing
from numpy import nan

from fireant import RollingMean
from fireant.tests.dataset.mocks import (
    ElectionOverElection,
    dimx1_date_df,
    dimx2_date_str_df,
    dimx2_date_str_ref_df,
    mock_dataset,
)


class RollingMeanTests(TestCase):
    def test_apply_to_timeseries(self):
        rolling_mean = RollingMean(mock_dataset.fields.wins, 3)
        result = rolling_mean.apply(dimx1_date_df, None)

        expected = pd.Series([nan, nan, 2.0, 2.0, 2.0, 2.0], name='$wins', index=dimx1_date_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim(self):
        rolling_mean = RollingMean(mock_dataset.fields.wins, 3)
        result = rolling_mean.apply(dimx2_date_str_df, None)

        expected = pd.Series(
            [nan, nan, nan, nan, nan, 2 / 3, 4 / 3, 2 / 3, 4 / 3, 4 / 3, 2 / 3, 4 / 3, 2 / 3],
            name='$wins',
            index=dimx2_date_str_df.index,
        )
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        rolling_mean = RollingMean(mock_dataset.fields.wins, 3)
        result = rolling_mean.apply(dimx2_date_str_ref_df, ElectionOverElection(mock_dataset.fields.timestamp))

        expected = pd.Series(
            [nan, nan, nan, nan, 4 / 3, 0.0, 4 / 3, 2 / 3, 4 / 3, 4 / 3, 2 / 3],
            name='$wins_eoe',
            index=dimx2_date_str_ref_df.index,
        )
        pandas.testing.assert_series_equal(expected, result)
