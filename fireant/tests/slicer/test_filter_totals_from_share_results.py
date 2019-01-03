from unittest import TestCase

import pandas as pd
import pandas.testing
from datetime import timedelta

from fireant.slicer.totals import scrub_totals_from_share_results
from fireant.tests.slicer.mocks import (
    cat_dim_df,
    cat_dim_totals_df,
    cont_cat_dim_df,
    cont_cat_dim_totals_df,
    cont_uni_dim_all_totals_df,
    cont_uni_dim_df,
    multi_metric_df,
    slicer,
)

TIMESTAMP_UPPERBOUND = pd.Timestamp.max - timedelta(seconds=1)


class ScrubTotalsTests(TestCase):
    def ignore_dimensionless_result_sets(self):
        result = scrub_totals_from_share_results(multi_metric_df, [])

        expected = multi_metric_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_remove_totals_for_non_rollup_dimensions(self):
        result = scrub_totals_from_share_results(cat_dim_totals_df, [slicer.dimensions.political_party])

        expected = cat_dim_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_remove_totals_for_non_rollup_dimensions_with_multiindex(self):
        result = scrub_totals_from_share_results(cont_cat_dim_totals_df, [slicer.dimensions.timestamp,
                                                                          slicer.dimensions.political_party])

        expected = cont_cat_dim_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_remove_totals_for_non_rollup_dimensions_with_multiindex_and_multiple_totals(self):
        result = scrub_totals_from_share_results(cont_uni_dim_all_totals_df, [slicer.dimensions.timestamp,
                                                                              slicer.dimensions.political_party])

        expected = cont_uni_dim_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions(self):
        result = scrub_totals_from_share_results(cat_dim_totals_df, [slicer.dimensions.political_party.rollup()])

        expected = cat_dim_totals_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex(self):
        result = scrub_totals_from_share_results(cont_cat_dim_totals_df, [slicer.dimensions.timestamp,
                                                                          slicer.dimensions.political_party.rollup()])

        expected = cont_cat_dim_totals_df

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex_and_multiple_totals(self):
        result = scrub_totals_from_share_results(cont_uni_dim_all_totals_df, [slicer.dimensions.timestamp,
                                                                              slicer.dimensions.political_party.rollup()])

        expected = cont_uni_dim_all_totals_df.loc[:TIMESTAMP_UPPERBOUND]

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex_and_multiple_totals_reversed(self):
        result = scrub_totals_from_share_results(cont_uni_dim_all_totals_df, [slicer.dimensions.timestamp.rollup(),
                                                                              slicer.dimensions.political_party])

        expected = cont_uni_dim_all_totals_df.loc[(slice(None), slice('1', '2')), :] \
            .append(cont_uni_dim_all_totals_df.iloc[-1])

        pandas.testing.assert_frame_equal(result, expected)

    def test_do_not_remove_totals_for_rollup_dimensions_with_multiindex_and_all_rolled_up(self):
        result = scrub_totals_from_share_results(cont_uni_dim_all_totals_df, [slicer.dimensions.timestamp.rollup(),
                                                                              slicer.dimensions.political_party.rollup()])

        expected = cont_uni_dim_all_totals_df

        pandas.testing.assert_frame_equal(result, expected)
