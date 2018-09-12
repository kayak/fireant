import copy
from unittest import TestCase

import numpy as np
import pandas as pd
import pandas.testing

from fireant.slicer.widgets.pandas import Pandas
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


class PandasTransformerTests(TestCase):
    maxDiff = None

    def test_single_metric(self):
        result = Pandas(slicer.metrics.votes) \
            .transform(single_metric_df, slicer, [], [])

        expected = single_metric_df.copy()[[fm('votes')]]
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_multiple_metrics(self):
        result = Pandas(slicer.metrics.votes, slicer.metrics.wins) \
            .transform(multi_metric_df, slicer, [], [])

        expected = multi_metric_df.copy()[[fm('votes'), fm('wins')]]
        expected.columns = ['Votes', 'Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_multiple_metrics_reversed(self):
        result = Pandas(slicer.metrics.wins, slicer.metrics.votes) \
            .transform(multi_metric_df, slicer, [], [])

        expected = multi_metric_df.copy()[[fm('wins'), fm('votes')]]
        expected.columns = ['Wins', 'Votes']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_time_series_dim(self):
        result = Pandas(slicer.metrics.wins) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_time_series_dim_with_operation(self):
        result = Pandas(CumSum(slicer.metrics.votes)) \
            .transform(cont_dim_operation_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_operation_df.copy()[[fm('cumsum(votes)')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['CumSum(Votes)']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_cat_dim(self):
        result = Pandas(slicer.metrics.wins) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_uni_dim(self):
        result = Pandas(slicer.metrics.wins) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        expected = uni_dim_df.copy() \
            .set_index(fd('candidate_display'), append=True) \
            .reset_index(fd('candidate'), drop=True) \
            [[fm('wins')]]
        expected.index.names = ['Candidate']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_uni_dim_no_display_definition(self):
        import copy
        candidate = copy.copy(slicer.dimensions.candidate)

        uni_dim_df_copy = uni_dim_df.copy()
        del uni_dim_df_copy[fd(slicer.dimensions.candidate.display.key)]
        del candidate.display

        result = Pandas(slicer.metrics.wins) \
            .transform(uni_dim_df_copy, slicer, [candidate], [])

        expected = uni_dim_df_copy.copy()[[fm('wins')]]
        expected.index.names = ['Candidate']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_multi_dims_time_series_and_uni(self):
        result = Pandas(slicer.metrics.wins) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=False)[[fm('wins')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_transpose_single_dimension(self):
        result = Pandas(slicer.metrics.wins, transpose=True) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.transpose()

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_single_dimension_transposes_data_frame(self):
        result = Pandas(slicer.metrics.wins, pivot=[slicer.dimensions.political_party]) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.transpose()

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_cat(self):
        result = Pandas(slicer.metrics.wins, pivot=[slicer.dimensions.political_party]) \
            .transform(cont_cat_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.political_party], [])

        expected = cont_cat_dim_df.copy()[[fm('wins')]]
        expected = expected.unstack(level=[1]).fillna(value='')
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        pandas.testing.assert_frame_equal(expected, result)

    def test_time_series_ref(self):
        result = Pandas(slicer.metrics.votes) \
            .transform(cont_uni_dim_ref_df, slicer,
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
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_metric_format(self):
        import copy
        votes = copy.copy(slicer.metrics.votes)
        votes.prefix = '$'
        votes.suffix = '€'
        votes.precision = 2

        # divide the data frame by 3 to get a repeating decimal so we can check precision
        result = Pandas(votes) \
            .transform(cont_dim_df / 3, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('votes')]]
        expected[fm('votes')] = ['${0:,.2f}€'.format(x)
                                 for x in expected[fm('votes')] / 3]
        expected.index.names = ['Timestamp']
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_nan_in_metrics(self):
        cat_dim_df_with_nan = cat_dim_df.copy()
        cat_dim_df_with_nan['$m$wins'] = cat_dim_df_with_nan['$m$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.nan

        result = Pandas(slicer.metrics.wins) \
            .transform(cat_dim_df_with_nan, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_inf_in_metrics(self):
        cat_dim_df_with_nan = cat_dim_df.copy()
        cat_dim_df_with_nan['$m$wins'] = cat_dim_df_with_nan['$m$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        result = Pandas(slicer.metrics.wins) \
            .transform(cat_dim_df_with_nan, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_neginf_in_metrics(self):
        cat_dim_df_with_nan = cat_dim_df.copy()
        cat_dim_df_with_nan['$m$wins'] = cat_dim_df_with_nan['$m$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        result = Pandas(slicer.metrics.wins) \
            .transform(cat_dim_df_with_nan, slicer, [slicer.dimensions.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_inf_in_metrics_with_precision_zero(self):
        cat_dim_df_with_nan = cat_dim_df.copy()
        cat_dim_df_with_nan['$m$wins'] = cat_dim_df_with_nan['$m$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        slicer_modified = copy.deepcopy(slicer)
        slicer_modified.metrics.wins.precision = 0

        result = Pandas(slicer_modified.metrics.wins) \
            .transform(cat_dim_df_with_nan, slicer_modified, [slicer_modified.dimensions.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[fm('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected['$m$wins'] = ['6', '0', 'Inf']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)


class PandasTransformerSortTests(TestCase):
    def test_multiple_metrics_sort_index_asc(self):
        result = Pandas(slicer.metrics.wins, sort=[0]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_index()

        pandas.testing.assert_frame_equal(expected, result)

    def test_multiple_metrics_sort_index_desc(self):
        result = Pandas(slicer.metrics.wins, sort=[0], ascending=[False]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_index(ascending=False)

        pandas.testing.assert_frame_equal(expected, result)

    def test_multiple_metrics_sort_value_asc(self):
        result = Pandas(slicer.metrics.wins, sort=[1]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_values(['Wins'])

        pandas.testing.assert_frame_equal(expected, result)

    def test_multiple_metrics_sort_value_desc(self):
        result = Pandas(slicer.metrics.wins, sort=[1], ascending=[False]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_values(['Wins'], ascending=False)

        pandas.testing.assert_frame_equal(expected, result)

    def test_multiple_metrics_sort_index_and_value(self):
        result = Pandas(slicer.metrics.wins, sort=[-0, 1]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index()
        expected = expected.sort_values(['Timestamp', 'Wins'], ascending=[True, False])
        expected = expected.set_index('Timestamp')

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_index_asc(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[0]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.sort_index()

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_index_desc(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[0], ascending=[False]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.sort_index(ascending=False)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_first_metric_asc(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[1]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.reset_index()
        expected = expected.sort_values(['California'])
        expected = expected.set_index('Timestamp')

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_first_metric_desc(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[1], ascending=[False]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.reset_index()
        expected = expected.sort_values(['California'], ascending=[False])
        expected = expected.set_index('Timestamp')

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_second_metric_asc(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[2]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.reset_index()
        expected = expected.sort_values(['Texas'], ascending=[True])
        expected = expected.set_index('Timestamp')

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_second_metric_desc(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[2], ascending=[False]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.reset_index()
        expected = expected.sort_values(['Texas'], ascending=[False])
        expected = expected.set_index('Timestamp')

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_uni_with_sort_index_and_columns(self):
        result = Pandas(slicer.metrics.votes, pivot=[slicer.dimensions.state], sort=[0, 2], ascending=[True, False]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=True)[[fm('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['California', 'Texas']
        expected.columns.names = ['State']

        expected = expected.reset_index()
        expected = expected.sort_values(['Timestamp', 'Texas'], ascending=[True, False])
        expected = expected.set_index('Timestamp')

        pandas.testing.assert_frame_equal(expected, result)

    def test_multi_dims_time_series_and_cat_sort_index_level_0_asc(self):
        result = Pandas(slicer.metrics.wins, sort=[0]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=False)[[fm('wins')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index()
        expected = expected.sort_values(['Timestamp'])
        expected = expected.set_index(['Timestamp', 'State'])

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_cat_sort_index_level_1_desc(self):
        result = Pandas(slicer.metrics.wins, sort=[1], ascending=[False]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=False)[[fm('wins')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index()
        expected = expected.sort_values(['State'], ascending=[False])
        expected = expected.set_index(['Timestamp', 'State'])

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_multi_dims_time_series_and_cat_sort_index_and_values(self):
        result = Pandas(slicer.metrics.wins, sort=[0, 2], ascending=[False, True]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        expected = cont_uni_dim_df.copy() \
            .set_index(fd('state_display'), append=True) \
            .reset_index(fd('state'), drop=False)[[fm('wins')]]
        expected.index.names = ['Timestamp', 'State']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index()
        expected = expected.sort_values(['Timestamp', 'Wins'], ascending=[False, True])
        expected = expected.set_index(['Timestamp', 'State'])

        pandas.testing.assert_frame_equal(expected, result)

    def test_empty_sort_array_is_ignored(self):
        result = Pandas(slicer.metrics.wins, sort=[]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)


    def test_sort_value_greater_than_number_of_columns_is_ignored(self):
        result = Pandas(slicer.metrics.wins, sort=[5]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        expected = cont_dim_df.copy()[[fm('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)
