from unittest import TestCase

import pandas as pd

from fireant.slicer.widgets import CSV
from fireant.tests.slicer.mocks import (
    CumSum,
    ElectionOverElection,
    cat_dim_df,
    cont_cat_dim_df,
    cont_dim_df,
    cont_dim_operation_df,
    cont_uni_dim_df,
    cont_uni_dim_ref_df,
    multi_metric_df,
    single_metric_df,
    slicer,
    uni_dim_df,
)
from fireant.utils import (
    format_dimension_key as fd,
    format_metric_key as fm,
)


class CSVWidgetTests(TestCase):
    maxDiff = None

    def test_single_metric(self):
        result = CSV(slicer.metrics.votes) \
            .transform(single_metric_df, slicer, [], [])

        expected = single_metric_df.copy()[[fm('votes')]]
        expected.columns = ['Votes']

        self.assertEqual(result, expected.to_csv())

    def test_multiple_metrics(self):
        result = CSV(slicer.metrics.votes, slicer.metrics.wins) \
            .transform(multi_metric_df, slicer, [], [])

        expected = multi_metric_df.copy()[[fm('votes'), fm('wins')]]
        expected.columns = ['Votes', 'Wins']

        self.assertEqual(result, expected.to_csv())

    def test_multiple_metrics_reversed(self):
        result = CSV(slicer.metrics.wins, slicer.metrics.votes) \
            .transform(multi_metric_df, slicer, [], [])

        expected = multi_metric_df.copy()[[fm('wins'), fm('votes')]]
        expected.columns = ['Wins', 'Votes']

        self.assertEqual(result, expected.to_csv())

    def test_time_series_dim(self):
        result = CSV(slicer.metrics.wins) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']

        self.assertEqual(result, expected.to_csv())

    def test_time_series_dim_with_operation(self):
        result = CSV(CumSum(slicer.metrics.votes)) \
            .transform(cont_dim_operation_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_operation_df.copy()[[fm('cumsum(votes)')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['CumSum(Votes)']

        self.assertEqual(result, expected.to_csv())

    def test_cat_dim(self):
        result = CSV(slicer.metrics.wins) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']

        self.assertEqual(result, expected.to_csv())

    def test_uni_dim(self):
        result = CSV(slicer.metrics.wins) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        expected = uni_dim_df.copy() \
            .set_index(fd('candidate_display'), append=True) \
            .reset_index(fd('candidate'), drop=True)[[fm('wins')]]
        expected.index.names = ['Candidate']
        expected.columns = ['Wins']

        self.assertEqual(result, expected.to_csv())

    def test_uni_dim_no_display_definition(self):
        import copy
        candidate = copy.copy(slicer.dimensions.candidate)
        candidate.display_key = None
        candidate.display_definition = None

        uni_dim_df_copy = uni_dim_df.copy()
        del uni_dim_df_copy[fd(slicer.dimensions.candidate.display_key)]

        result = CSV(slicer.metrics.wins) \
            .transform(uni_dim_df_copy, slicer, [candidate], [])

        expected = uni_dim_df_copy.copy()[[fm('wins')]]
        expected.index.names = ['Candidate']
        expected.columns = ['Wins']

        self.assertEqual(result, expected.to_csv())

    def test_multi_dims_time_series_and_uni(self):
        result = CSV(slicer.metrics.wins) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=False)[[fm('wins')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Wins']

        self.assertEqual(result, expected.to_csv())

    def test_pivoted_single_dimension_no_effect(self):
        result = CSV(slicer.metrics.wins, pivot=True) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']

        self.assertEqual(result, expected.to_csv())

    def test_pivoted_multi_dims_time_series_and_cat(self):
        result = CSV(slicer.metrics.wins, pivot=True) \
            .transform(cont_cat_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.political_party], [])

        expected = cont_cat_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected = expected.unstack(level=[1])

        self.assertEqual(result, expected.to_csv())

    def test_pivoted_multi_dims_time_series_and_uni(self):
        result = CSV(slicer.metrics.votes, pivot=True) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Votes']
        expected = expected.unstack(level=[1])

        self.assertEqual(result, expected.to_csv())

    def test_time_series_ref(self):
        result = CSV(slicer.metrics.votes) \
            .transform(cont_uni_dim_ref_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                                        ], [
                           ElectionOverElection(slicer.dimensions.timestamp)
                       ])

        expected = cont_uni_dim_ref_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes'), fm('votes_eoe')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Votes', 'Votes (EoE)']

        self.assertEqual(result, expected.to_csv())
