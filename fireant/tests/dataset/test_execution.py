from datetime import date
from unittest import (
    TestCase,
    skip,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import numpy as np
import pandas as pd
import pandas.testing
from pandas._testing import assert_frame_equal
from pypika import Query

from fireant import DayOverDay
from fireant.dataset.modifiers import Rollup, RollupValue
from fireant.dataset.totals import get_totals_marker_for_dtype
from fireant.queries.execution import (
    fetch_data,
    reduce_result_set,
)
from .mocks import (
    dimx0_metricx1_df,
    dimx1_date_df,
    dimx1_str_df,
    dimx1_str_totals_df,
    dimx2_date_str_df,
    dimx2_date_str_totals_df,
    dimx2_date_str_totalsx2_df,
    dimx2_str_num_df,
    dimx3_date_str_str_df,
    dimx3_date_str_str_totalsx3_df,
    mock_dataset,
    politicians_hint_table,
    politicians_table,
)
from ..test_formats import boolean_field, date_field, number_field, text_field

pd.set_option("display.expand_frame_repr", False)
metrics = ["$votes", "$wins", "$wins_with_style", "$turnout"]


def replace_totals(data_frame):
    index_names = data_frame.index.names

    raw = data_frame.reset_index()
    for name in index_names:
        marker = get_totals_marker_for_dtype(raw[name].dtype)
        raw[name].replace(marker, np.nan, inplace=True)

    return raw


class TestFetchData(TestCase):
    @classmethod
    def setUpClass(cls):
        query_a = Query.from_(politicians_table).select("*")
        query_b = Query.from_(politicians_hint_table).select("*")

        cls.test_dimensions = ()
        cls.test_queries = [query_a, query_b]

        cls.test_result_a = pd.DataFrame([{"a": 1.0}])
        cls.test_result_b = pd.DataFrame([{"b": 2.0}])

    @patch("fireant.queries.execution.reduce_result_set")
    def test_fetch_data(self, reduce_mock):
        database = MagicMock(max_result_set_size=1000)
        database.fetch_dataframes.return_value = [
            self.test_result_a,
            self.test_result_b,
        ]

        mocked_result = pd.DataFrame([{"a": 1.0}])
        reduce_mock.return_value = mocked_result

        max_rows_returned, result = fetch_data(database, self.test_queries, self.test_dimensions)

        pandas.testing.assert_frame_equal(mocked_result, result)
        database.fetch_dataframes.assert_called_with(
            'SELECT * FROM "politics"."politician"', 'SELECT * FROM "politics"."hints"', parse_dates={}
        )
        reduce_mock.assert_called_once_with([self.test_result_a, self.test_result_b], (), self.test_dimensions, ())

    @patch("fireant.queries.execution.reduce_result_set")
    def test_fetch_data_strips_rows_over_max_result_set_size(self, reduce_mock):
        database = MagicMock(max_result_set_size=2)
        database.fetch_dataframes.return_value = [
            pd.DataFrame([{"a": 1.0}, {"a": 2.0}, {"a": 3.0}]),
            pd.DataFrame([{"a": 1.0}]),
        ]
        reduce_mock.side_effect = lambda *args: args[0]  # let the reduce mock pass on the dataframes unchanged

        max_rows_returned, result = fetch_data(database, self.test_queries, self.test_dimensions)

        database.fetch_dataframes.assert_called_with(
            'SELECT * FROM "politics"."politician"', 'SELECT * FROM "politics"."hints"', parse_dates={}
        )
        assert_frame_equal(result[0], pd.DataFrame([{"a": 1.0}, {"a": 2.0}]))
        assert_frame_equal(result[1], pd.DataFrame([{"a": 1.0}]))
        self.assertEqual(max_rows_returned, 3)

    @patch("fireant.queries.execution.reduce_result_set")
    def test_fetch_data_with_date_dimensions(self, reduce_mock):
        database = MagicMock()
        database.fetch_dataframes.return_value = []
        dimensions = [number_field, date_field, boolean_field, text_field]

        fetch_data(database, self.test_queries, dimensions)

        database.fetch_dataframes.assert_called_with(
            'SELECT * FROM "politics"."politician"', 'SELECT * FROM "politics"."hints"', parse_dates={'$date': {}}
        )


class ReduceResultSetsTests(TestCase):
    def test_reduce_single_result_set_no_dimensions(self):
        expected = dimx0_metricx1_df
        raw_df = expected

        dimensions = ()
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_dimension(self):
        expected = dimx1_date_df
        raw_df = replace_totals(expected)

        dimensions = (mock_dataset.fields.timestamp,)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_1x_dimension(self):
        expected = dimx1_str_df
        raw_df = replace_totals(expected)

        dimensions = (mock_dataset.fields.political_party,)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_date_str_dimensions_dates(self):
        expected = dimx2_date_str_df
        raw_df = replace_totals(expected)

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        )
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_str_num_dimensions(self):
        expected = dimx2_str_num_df.sort_index()
        raw_df = replace_totals(expected)

        dimensions = (
            mock_dataset.fields.political_party,
            mock_dataset.fields["candidate-id"],
        )
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_date_str_str_dimensions(self):
        expected = dimx3_date_str_str_df
        raw_df = replace_totals(expected)

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
            mock_dataset.fields.state,
        )
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)


class ReduceResultSetsWithReferencesTests(TestCase):
    def test_reduce_delta_percent_result_set_with_zeros_in_reference_value(self):
        raw_df = pd.DataFrame(
            [[date(2019, 1, 2), 1], [date(2019, 1, 3), 2]],
            columns=["$timestamp", "$metric"],
        )
        ref_df = pd.DataFrame(
            [[date(2019, 1, 2), 2], [date(2019, 1, 3), 0]],
            columns=["$timestamp", "$metric_dod"],
        )

        expected = raw_df.copy()
        expected["$metric_dod_delta_percent"] = pd.Series([-50, np.nan], dtype=object)
        expected["$metric_dod"] = pd.Series([2, 0])
        expected.set_index("$timestamp", inplace=True)

        timestamp = mock_dataset.fields.timestamp
        reference_groups = ([DayOverDay(timestamp, delta_percent=True)],)
        dimensions = (timestamp,)
        result = reduce_result_set([raw_df, ref_df], reference_groups, dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_delta_result_with_non_aligned_index(self):
        raw_df = pd.DataFrame(
            [[date(2019, 1, 2), 1], [date(2019, 1, 3), 2]],
            columns=["$timestamp", "$metric"],
        )
        ref_df = pd.DataFrame([[date(2019, 1, 2), 2]], columns=["$timestamp", "$metric_dod"])

        expected = raw_df.copy()
        expected["$metric_dod_delta"] = pd.Series([-1.0, 2.0], dtype=object)
        expected["$metric_dod"] = pd.Series([2.0, np.nan])
        expected.set_index("$timestamp", inplace=True)

        timestamp = mock_dataset.fields.timestamp
        reference_groups = ([DayOverDay(timestamp, delta=True)],)
        dimensions = (timestamp,)
        result = reduce_result_set([raw_df, ref_df], reference_groups, dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_with_multiple_references_of_the_same_type_over_the_same_dimension(self):
        raw_df = pd.DataFrame(
            [[date(2019, 1, 2), 1], [date(2019, 1, 3), 2]],
            columns=["$timestamp", "$metric"],
        )
        ref_df = pd.DataFrame(
            [[date(2019, 1, 2), 2], [date(2019, 1, 3), 0]],
            columns=["$timestamp", "$metric_dod"],
        )

        expected = raw_df.copy()
        expected["$metric_dod"] = pd.Series([2, 0])
        expected["$metric_dod_delta"] = pd.Series([-1, 2], dtype=object)
        expected["$metric_dod_delta_percent"] = pd.Series([-50, np.nan], dtype=object)
        expected.set_index("$timestamp", inplace=True)

        timestamp = mock_dataset.fields.timestamp
        reference_groups = (
            [DayOverDay(timestamp), DayOverDay(timestamp, delta=True), DayOverDay(timestamp, delta_percent=True)],
        )
        dimensions = (timestamp,)
        result = reduce_result_set([raw_df, ref_df], reference_groups, dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_with_references_and_extra_values_in_reference(self):
        raw_df = pd.DataFrame(
            [[date(2019, 1, 2), 1], [date(2019, 1, 3), 9]],
            columns=["$timestamp", "$metric"],
        )
        ref_df = pd.DataFrame(
            [[date(2019, 1, 2), 7], [date(2019, 1, 4), 8]],
            columns=["$timestamp", "$metric_dod"],
        )
        expected_df = pd.DataFrame(
            [[date(2019, 1, 2), 1, 7], [date(2019, 1, 3), 9, np.nan]],
            columns=["$timestamp", "$metric", "$metric_dod"],
        )
        expected_df.set_index("$timestamp", inplace=True)

        timestamp = mock_dataset.fields.timestamp
        reference_groups = (
            [
                DayOverDay(timestamp),
            ],
        )
        dimensions = (timestamp,)
        result = reduce_result_set([raw_df, ref_df], reference_groups, dimensions, ())

        pandas.testing.assert_frame_equal(expected_df, result)


class ReduceResultSetsWithTotalsTests(TestCase):
    def test_reduce_single_result_set_with_str_dimension(self):
        expected = dimx1_str_totals_df
        raw_df = replace_totals(dimx1_str_df)
        totals_df = pd.merge(
            pd.DataFrame([RollupValue.CONSTANT], columns=["$political_party"]),
            pd.DataFrame([raw_df[metrics].sum(axis=0)]),
            how="outer",
            left_index=True,
            right_index=True,
        )

        dimensions = (Rollup(mock_dataset.fields.political_party),)
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_dimx2_date_str_totals_date(self):
        expected = dimx2_date_str_totalsx2_df.loc[(slice(None), slice("Democrat", "Republican")), :].append(
            dimx2_date_str_totalsx2_df.iloc[-1]
        )
        raw_df = replace_totals(dimx2_date_str_df)
        totals_df = pd.merge(
            pd.DataFrame([[RollupValue.CONSTANT, RollupValue.CONSTANT]], columns=["$timestamp", "$political_party"]),
            pd.DataFrame([raw_df[metrics].sum(axis=0)]),
            how="outer",
            left_index=True,
            right_index=True,
        )

        dimensions = (
            Rollup(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        )
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_date_str_dimensions_str_totals(self):
        expected = dimx2_date_str_totals_df
        raw_df = replace_totals(dimx2_date_str_df)
        totals_df = raw_df.groupby("$timestamp").sum().reset_index()
        totals_df["$political_party"] = '_FIREANT_ROLLUP_VALUE_'
        totals_df = totals_df[["$timestamp", "$political_party"] + metrics]

        dimensions = (
            mock_dataset.fields.timestamp,
            Rollup(mock_dataset.fields.political_party),
        )
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_dimx2_date_str_str_totals_date(self):
        expected = dimx3_date_str_str_totalsx3_df.loc[
            (
                slice(None),
                slice("Democrat", "Republican"),
                slice("California", "Texas"),
            ),
            :,
        ].append(dimx3_date_str_str_totalsx3_df.iloc[-1])

        raw_df = replace_totals(dimx3_date_str_str_df)
        totals_df = pd.merge(
            pd.DataFrame(
                [[RollupValue.CONSTANT, RollupValue.CONSTANT, RollupValue.CONSTANT]],
                columns=["$timestamp", "$political_party", "$state"],
            ),
            pd.DataFrame([raw_df[metrics].sum(axis=0)]),
            how="outer",
            left_index=True,
            right_index=True,
        )
        totals_df = totals_df[["$timestamp", "$political_party", "$state"] + metrics]

        dimensions = (
            Rollup(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
            mock_dataset.fields.state,
        )
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_date_str_str_dimensions_str1_totals(self):
        expected = (
            dimx3_date_str_str_totalsx3_df.loc[(slice(None), slice(None), slice("California", "Texas")), :]
            .append(dimx3_date_str_str_totalsx3_df.loc[(slice(None), "~~totals"), :].iloc[:-1])
            .sort_index()
        )

        raw_df = replace_totals(dimx3_date_str_str_df)
        totals_df = raw_df.groupby("$timestamp").sum().reset_index()
        totals_df["$political_party"] = RollupValue.CONSTANT
        totals_df["$state"] = RollupValue.CONSTANT
        totals_df = totals_df[["$timestamp", "$political_party", "$state"] + metrics]

        dimensions = (
            mock_dataset.fields.timestamp,
            Rollup(mock_dataset.fields.political_party),
            mock_dataset.fields.state,
        )
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_date_str_str_dimensions_str2_totals(self):
        expected = dimx3_date_str_str_totalsx3_df.loc[(slice(None), slice("Democrat", "Republican")), :]
        raw_df = replace_totals(dimx3_date_str_str_df)
        totals_df = raw_df.groupby(["$timestamp", "$political_party"]).sum().reset_index()
        totals_df["$state"] = RollupValue.CONSTANT
        totals_df = totals_df[["$timestamp", "$political_party", "$state"] + metrics]

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
            Rollup(mock_dataset.fields.state),
        )
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    @skip("BAN-2594")
    def test_reduce_single_result_set_with_date_str_str_dimensions_str1_totals_with_null_in_date_dim(
        self,
    ):
        index_names = list(dimx3_date_str_str_totalsx3_df.index.names)
        nulls = pd.DataFrame(
            [
                [np.nan, "d", "1", "Texas", 5, 0],
                [np.nan, "d", "2", "California", 2, 0],
                [np.nan, "i", "1", "Texas", 5, 0],
                [np.nan, "i", "2", "California", 7, 0],
                [np.nan, "r", "1", "Texas", 11, 0],
                [np.nan, "r", "2", "California", 3, 0],
            ],
            columns=index_names + list(dimx3_date_str_str_totalsx3_df.columns),
        )
        nulls_totals = pd.DataFrame([nulls[metrics].sum()])
        nulls_totals[index_names[0]] = np.nan
        nulls_totals[index_names[1]] = "~~totals"
        nulls_totals[index_names[2]] = "~~totals"

        expected = (
            dimx3_date_str_str_totalsx3_df.loc[(slice(None), slice(None), slice("1", "2")), :]
            .append(dimx3_date_str_str_totalsx3_df.loc[(slice(None), "~~totals"), :].iloc[:-1])
            .append(nulls.set_index(index_names))
            .append(nulls_totals.set_index(index_names))
            .sort_index()
        )
        raw_df = replace_totals(dimx3_date_str_str_df)
        raw_df = nulls.append(raw_df).sort_values(["$timestamp", "$political_party", "$state"])

        totals_df = raw_df.groupby("$timestamp").sum().reset_index()
        null_totals_df = pd.DataFrame([raw_df[raw_df["$timestamp"].isnull()][metrics].sum()])
        null_totals_df["$timestamp"] = None
        totals_df = totals_df.append(null_totals_df)
        totals_df["$political_party"] = RollupValue.CONSTANT
        totals_df["$state"] = RollupValue.CONSTANT
        totals_df = totals_df[["$timestamp", "$political_party", "$state"] + metrics]

        dimensions = (
            mock_dataset.fields.timestamp,
            Rollup(mock_dataset.fields.political_party),
            mock_dataset.fields.state,
        )
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)
