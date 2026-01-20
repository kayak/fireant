from unittest import TestCase

import pandas as pd
from fireant import (
    CumMean,
    CumProd,
    CumSum,
    Field,
)


class CumulativeOperationTests(TestCase):
    def test_with_empty_dataset_single_dimension(self):
        metric = Field("test", None, label="Test")
        idx = pd.Index([], name="level0")
        frame = pd.DataFrame([], index=idx, columns=["$test"], dtype=float)
        expected = pd.Series([], index=idx, name="$test", dtype=float)

        for op in (CumSum, CumMean, CumProd):
            with self.subTest(op.__name__):
                op_cum = op(metric)
                result = op_cum.apply(frame, None)
                pd.testing.assert_series_equal(result, expected)

    def test_with_empty_dataset_multiple_dimensions(self):
        metric = Field("test", None, label="Test")
        mi = pd.MultiIndex.from_product([[], []], names=("level0", "level1"))
        frame = pd.DataFrame([], index=mi, columns=["$test"], dtype=float)
        expected = pd.Series([], index=mi, name="$test", dtype=float)

        for op in (CumSum, CumMean, CumProd):
            with self.subTest(op.__name__):
                op_cum = op(metric)
                result = op_cum.apply(frame, None)
                pd.testing.assert_series_equal(result, expected)
