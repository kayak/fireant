from datetime import timedelta
from unittest import TestCase

import pandas as pd
import pandas.testing

from fireant.dataset.modifiers import Rollup
from fireant.dataset.totals import scrub_totals_from_share_results
from fireant.tests.dataset.mocks import (
    dimx0_metricx2_df,
    dimx1_str_df,
    dimx1_str_totals_df,
    dimx2_date_str_df,
    dimx2_date_str_totals_df,
    dimx2_date_str_totalsx2_df,
    mock_dataset,
)

TIMESTAMP_UPPERBOUND = pd.Timestamp.max - timedelta(seconds=1)


class ScrubTotalsTests(TestCase):
    def ignore_dimensionless_result_sets(self):
        result = scrub_totals_from_share_results(dimx0_metricx2_df, [])

        expected = dimx0_metricx2_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_remove_totals_for_non_rollup_dimensions(self):
        result = scrub_totals_from_share_results(dimx1_str_totals_df, [mock_dataset.fields.political_party])

        expected = dimx1_str_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_remove_totals_for_non_rollup_dimensions_with_multiindex(self):
        result = scrub_totals_from_share_results(
            dimx2_date_str_totals_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        )

        expected = dimx2_date_str_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_remove_totals_for_non_rollup_dimensions_with_multiindex_and_multiple_totals(self):
        result = scrub_totals_from_share_results(
            dimx2_date_str_totalsx2_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        )

        expected = dimx2_date_str_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions(self):
        result = scrub_totals_from_share_results(dimx1_str_totals_df, [Rollup(mock_dataset.fields.political_party)])

        expected = dimx1_str_totals_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex(self):
        result = scrub_totals_from_share_results(
            dimx2_date_str_totals_df, [mock_dataset.fields.timestamp, Rollup(mock_dataset.fields.political_party)]
        )

        expected = dimx2_date_str_totals_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex_and_lower_dimension_totals(self):
        result = scrub_totals_from_share_results(
            dimx2_date_str_totalsx2_df, [mock_dataset.fields.timestamp, Rollup(mock_dataset.fields.political_party)]
        )

        expected = dimx2_date_str_totalsx2_df.loc[:TIMESTAMP_UPPERBOUND]

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex_and_higher_dimension_totals(self):
        result = scrub_totals_from_share_results(
            dimx2_date_str_totalsx2_df, [Rollup(mock_dataset.fields.timestamp), mock_dataset.fields.political_party]
        )

        expected = dimx2_date_str_totalsx2_df.loc[(slice(None), slice('Democrat', 'Republican')), :].append(
            dimx2_date_str_totalsx2_df.iloc[-1]
        )

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex_and_all_rolled_up(self):
        result = scrub_totals_from_share_results(
            dimx2_date_str_totalsx2_df,
            [Rollup(mock_dataset.fields.timestamp), Rollup(mock_dataset.fields.political_party)],
        )

        expected = dimx2_date_str_totalsx2_df

        pandas.testing.assert_frame_equal(result, expected)
