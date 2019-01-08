from unittest import TestCase

import numpy as np
import pandas as pd
import pandas.testing

from fireant import RollingMean
from fireant.tests.slicer.mocks import (
    slicer,
    cont_dim_df,
    cont_uni_dim_df,
    cont_uni_dim_ref_df,
    ElectionOverElection,
)


class RollingMeanTests(TestCase):
    def test_apply_to_timeseries(self):
        rolling_mean = RollingMean(slicer.metrics.wins, 3)
        result = rolling_mean.apply(cont_dim_df, None)

        expected = pd.Series([np.nan, np.nan, 2.0, 2.0, 2.0, 2.0],
                             name='$m$wins',
                             index=cont_dim_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim(self):
        rolling_mean = RollingMean(slicer.metrics.wins, 3)
        result = rolling_mean.apply(cont_uni_dim_df, None)

        expected = pd.Series([np.nan, np.nan, np.nan, np.nan, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                             name='$m$wins',
                             index=cont_uni_dim_df.index)
        pandas.testing.assert_series_equal(expected, result)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        rolling_mean = RollingMean(slicer.metrics.wins, 3)
        result = rolling_mean.apply(cont_uni_dim_ref_df, ElectionOverElection(slicer.dimensions.timestamp))

        expected = pd.Series([np.nan, np.nan, np.nan, np.nan, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                             name='$m$wins_eoe',
                             index=cont_uni_dim_ref_df.index)
        pandas.testing.assert_series_equal(expected, result)
