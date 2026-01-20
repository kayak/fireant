from unittest import TestCase

import numpy as np
import pandas as pd

from fireant.queries.pandas_workaround import df_subtract


class TestSubtract(TestCase):
    def test_subtract_partially_aligned_multi_index_dataframes_with_nans(self):
        df0 = pd.DataFrame(
            data=[
                [1, 2],
                [3, 4],
                [5, 6],
                [7, 8],
                [9, 10],
                [11, 12],
                [13, 14],
                [15, 16],
                [17, 18],
            ],
            columns=["happy", "sad"],
            index=pd.MultiIndex.from_product([["a", "b", None], [0, 1, np.nan]], names=["l0", "l1"]),
        )
        df1 = pd.DataFrame(
            data=[
                [1, 2],
                [3, 4],
                [5, 6],
                [7, 8],
                [9, 10],
                [11, 12],
                [13, 14],
                [15, 16],
                [17, 18],
            ],
            columns=["happy", "sad"],
            index=pd.MultiIndex.from_product([["b", "c", None], [1, 2, np.nan]], names=["l0", "l1"]),
        )

        result = df_subtract(df0, df1, fill_value=0)
        expected = pd.DataFrame.from_records(
            [
                ["a", 0, 1 - 0, 2 - 0],
                ["a", 1, 3 - 0, 4 - 0],
                ["a", np.nan, 5 - 0, 6 - 0],
                ["b", 0, 7 - 0, 8 - 0],
                ["b", 1, 9 - 1, 10 - 2],
                ["b", np.nan, 11 - 5, 12 - 6],
                [np.nan, 0, 13 - 0, 14 - 0],
                [np.nan, 1, 15 - 13, 16 - 14],
                [np.nan, np.nan, 17 - 17, 18 - 18],
                ["b", 2, 0 - 3, 0 - 4],
                ["c", 1, 0 - 7, 0 - 8],
                ["c", 2, 0 - 9, 0 - 10],
                ["c", np.nan, 0 - 11, 0 - 12],
                [np.nan, 2, 0 - 15, 0 - 16],
            ],
            columns=["l0", "l1", "happy", "sad"],
        ).set_index(["l0", "l1"])

        pd.testing.assert_frame_equal(expected, result, check_index_type=False)
        self.assertTrue(result.index.is_unique)
