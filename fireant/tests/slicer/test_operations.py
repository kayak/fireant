from unittest import TestCase

import pandas as pd
import pandas.testing
from fireant import (
    CumMean,
    CumProd,
    CumSum,
)
from fireant.tests.slicer.mocks import (
    cont_dim_df,
    cont_uni_dim_df,
    cont_uni_dim_ref_df,
    slicer,
)


class CumSumTests(TestCase):
    def test_apply_to_timeseries(self):
        cumsum = CumSum(slicer.metrics.wins)
        result = cumsum.apply(cont_dim_df)

        expected = pd.Series([2, 4, 6, 8, 10, 12],
                             name='wins',
                             index=cont_dim_df.index)
        pandas.testing.assert_series_equal(result, expected)

    def test_apply_to_timeseries_with_uni_dim(self):
        cumsum = CumSum(slicer.metrics.wins)
        result = cumsum.apply(cont_uni_dim_df)

        expected = pd.Series([1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
                             name='wins',
                             index=cont_uni_dim_df.index)
        pandas.testing.assert_series_equal(result, expected)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        cumsum = CumSum(slicer.metrics.wins)
        result = cumsum.apply(cont_uni_dim_ref_df)

        expected = pd.Series([1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
                             name='wins',
                             index=cont_uni_dim_ref_df.index)
        pandas.testing.assert_series_equal(result, expected)


class CumProdTests(TestCase):
    def test_apply_to_timeseries(self):
        cumprod = CumProd(slicer.metrics.wins)
        result = cumprod.apply(cont_dim_df)

        expected = pd.Series([2, 4, 8, 16, 32, 64],
                             name='wins',
                             index=cont_dim_df.index)
        pandas.testing.assert_series_equal(result, expected)

    def test_apply_to_timeseries_with_uni_dim(self):
        cumprod = CumProd(slicer.metrics.wins)
        result = cumprod.apply(cont_uni_dim_df)

        expected = pd.Series([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                             name='wins',
                             index=cont_uni_dim_df.index)
        pandas.testing.assert_series_equal(result, expected)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        cumprod = CumProd(slicer.metrics.wins)
        result = cumprod.apply(cont_uni_dim_ref_df)

        expected = pd.Series([1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                             name='wins',
                             index=cont_uni_dim_ref_df.index)
        pandas.testing.assert_series_equal(result, expected)


class CumMeanTests(TestCase):
    def test_apply_to_timeseries(self):
        cummean = CumMean(slicer.metrics.votes)
        result = cummean.apply(cont_dim_df)

        expected = pd.Series([15220449.0, 15941233.0, 17165799.3, 18197903.25, 18672764.6, 18612389.3],
                             name='votes',
                             index=cont_dim_df.index)
        pandas.testing.assert_series_equal(result, expected)

    def test_apply_to_timeseries_with_uni_dim(self):
        cummean = CumMean(slicer.metrics.votes)
        result = cummean.apply(cont_uni_dim_df)

        expected = pd.Series([5574387.0, 9646062.0, 5903886.0, 10037347.0, 6389131.0, 10776668.3,
                              6793838.5, 11404064.75, 7010664.2, 11662100.4, 6687706.0, 11924683.3],
                             name='votes',
                             index=cont_uni_dim_df.index)
        pandas.testing.assert_series_equal(result, expected)

    def test_apply_to_timeseries_with_uni_dim_and_ref(self):
        cummean = CumMean(slicer.metrics.votes)
        result = cummean.apply(cont_uni_dim_ref_df)

        expected = pd.Series([6233385.0, 10428632.0, 6796503.0, 11341971.5, 7200322.3,
                              11990065.6, 7369733.5, 12166110.0, 6910369.8, 12380407.6],
                             name='votes',
                             index=cont_uni_dim_ref_df.index)
        pandas.testing.assert_series_equal(result, expected)
