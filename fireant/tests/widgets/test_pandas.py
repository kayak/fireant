import copy
from unittest import TestCase

import numpy as np
import pandas as pd
import pandas.testing

from fireant.tests.dataset.mocks import (
    CumSum,
    ElectionOverElection,
    dimx0_metricx1_df,
    dimx0_metricx2_df,
    dimx1_date_df,
    dimx1_date_operation_df,
    dimx1_str_df,
    dimx2_date_str_df,
    dimx2_date_str_ref_df,
    mock_dataset,
    no_index_df,
)
from fireant.utils import alias_selector as f
from fireant.widgets.pandas import Pandas


def _format_float(x):
    if pd.isnull(x):
        return ''
    if x in [np.inf, -np.inf]:
        return 'Inf'
    return '{:,.0f}'.format(x)


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PandasTransformerTests(TestCase):
    maxDiff = None

    def test_metricx1(self):
        result = Pandas(mock_dataset.fields.votes) \
            .transform(dimx0_metricx1_df, mock_dataset, [], [])

        expected = dimx0_metricx1_df.copy()[[f('votes')]]
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2(self):
        result = Pandas(mock_dataset.fields.votes, mock_dataset.fields.wins) \
            .transform(dimx0_metricx2_df, mock_dataset, [], [])

        expected = dimx0_metricx2_df.copy()[[f('votes'), f('wins')]]
        expected.columns = ['Votes', 'Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_reversed(self):
        result = Pandas(mock_dataset.fields.wins, mock_dataset.fields.votes) \
            .transform(dimx0_metricx2_df, mock_dataset, [], [])

        expected = dimx0_metricx2_df.copy()[[f('wins'), f('votes')]]
        expected.columns = ['Wins', 'Votes']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_date(self):
        result = Pandas(mock_dataset.fields.wins) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_date_with_operation(self):
        result = Pandas(CumSum(mock_dataset.fields.votes)) \
            .transform(dimx1_date_operation_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_operation_df.copy()[[f('cumsum(votes)')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['CumSum(Votes)']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_str(self):
        result = Pandas(mock_dataset.fields.wins) \
            .transform(dimx1_str_df, mock_dataset, [mock_dataset.fields.political_party], [])

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_int(self):
        result = Pandas(mock_dataset.fields.wins) \
            .transform(dimx1_str_df, mock_dataset, [mock_dataset.fields['candidate-id']], [])

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index.names = ['Candidate ID']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx2_date_str(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        result = Pandas(mock_dataset.fields.wins) \
            .transform(dimx2_date_str_df, mock_dataset, dimensions, [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_transpose_dimx2_str(self):
        result = Pandas(mock_dataset.fields.wins, transpose=True) \
            .transform(dimx1_str_df, mock_dataset, [mock_dataset.fields.political_party], [])

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.transpose()
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx1_str_transposes_data_frame(self):
        result = Pandas(mock_dataset.fields.wins, pivot=[mock_dataset.fields.political_party]) \
            .transform(dimx1_str_df, mock_dataset, [mock_dataset.fields.political_party], [])

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.transpose()
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str(self):
        result = Pandas(mock_dataset.fields.wins, pivot=[mock_dataset.fields.political_party]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_time_series_ref(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = Pandas(mock_dataset.fields.votes) \
            .transform(dimx2_date_str_ref_df, mock_dataset,
                       dimensions, references)

        expected = dimx2_date_str_ref_df.copy()[[f('votes'), f('votes_eoe')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Votes', 'Votes EoE']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metric_format(self):
        import copy
        votes = copy.copy(mock_dataset.fields.votes)
        votes.prefix = '$'
        votes.suffix = '€'
        votes.precision = 2

        # divide the data frame by 3 to get a repeating decimal so we can check precision
        result = Pandas(votes) \
            .transform(dimx1_date_df / 3, mock_dataset, [mock_dataset.fields.timestamp], [])

        f_votes = f('votes')
        expected = dimx1_date_df.copy()[[f_votes]]
        expected[f_votes] = ['${0:,.2f}€'.format(x)
                             for x in expected[f_votes] / 3]
        expected.index.names = ['Timestamp']
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_nan_in_metrics(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.nan

        result = Pandas(mock_dataset.fields.wins) \
            .transform(cat_dim_df_with_nan, mock_dataset, [mock_dataset.fields.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_inf_in_metrics(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        result = Pandas(mock_dataset.fields.wins) \
            .transform(cat_dim_df_with_nan, mock_dataset, [mock_dataset.fields.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_neginf_in_metrics(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        result = Pandas(mock_dataset.fields.wins) \
            .transform(cat_dim_df_with_nan, mock_dataset, [mock_dataset.fields.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_inf_in_metrics_with_precision_zero(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        slicer_modified = copy.deepcopy(mock_dataset)
        slicer_modified.fields.wins.precision = 0

        result = Pandas(slicer_modified.fields.wins) \
            .transform(cat_dim_df_with_nan, slicer_modified, [slicer_modified.fields.political_party], [])

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected['$wins'] = ['6', '0', 'Inf']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)


class PandasTransformerSortTests(TestCase):
    def test_metricx2_sort_index_asc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_index()
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_index_desc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0], ascending=[False]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_index(ascending=False)
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_value_asc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[1]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_values(['Wins'])
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_value_desc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[1], ascending=[False]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_values(['Wins'], ascending=False)
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_index_and_value(self):
        result = Pandas(mock_dataset.fields.wins, sort=[-0, 1]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index() \
            .sort_values(['Timestamp', 'Wins'], ascending=[True, False]) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_index_asc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.sort_index()
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_index_desc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0],
                        ascending=[False]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.sort_index(ascending=False)
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_first_metric_asc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[1]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Democrat']) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_metric_desc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[1],
                        ascending=[False]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Democrat'], ascending=False) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_metric_asc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[1]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Democrat'], ascending=True) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx1_metricx2(self):
        result = Pandas(mock_dataset.fields.votes, mock_dataset.fields.wins, pivot=[mock_dataset.fields.timestamp]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes'), f('wins')]]
        expected = expected.unstack(level=0)
        expected.index.names = ['Party']
        expected.columns = pd.MultiIndex.from_product(
              [
                  ['Votes', 'Wins'],
                  pd.DatetimeIndex(['1996-01-01',
                                    '2000-01-01',
                                    '2004-01-01',
                                    '2008-01-01',
                                    '2012-01-01',
                                    '2016-01-01']),
              ],
              names=['Metrics', 'Timestamp'],
        )

        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_second_metric_desc(self):
        result = Pandas(mock_dataset.fields.votes,
                        pivot=[mock_dataset.fields.political_party],
                        sort=1,
                        ascending=False) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Democrat'], ascending=False) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_index_and_columns(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0, 2],
                        ascending=[True, False]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Timestamp', 'Democrat'], ascending=[True, False]) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_use_first_value_for_ascending_when_arg_has_invalid_length(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0, 2],
                        ascending=[True]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Timestamp', 'Democrat'], ascending=True) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_use_pandas_default_for_ascending_when_arg_empty_list(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0, 2],
                        ascending=[]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index() \
            .sort_values(['Timestamp', 'Democrat'], ascending=None) \
            .set_index('Timestamp')
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx2_date_str_sort_index_level_0_default_ascending(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index() \
            .sort_values(['Timestamp']) \
            .set_index(['Timestamp', 'Party'])
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx2_date_str_sort_index_level_0_asc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0], ascending=True) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index() \
            .sort_values(['Timestamp'], ascending=True) \
            .set_index(['Timestamp', 'Party'])
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_sort_index_level_1_desc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[1], ascending=[False]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index() \
            .sort_values(['Party'], ascending=[False]) \
            .set_index(['Timestamp', 'Party'])
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_sort_index_and_values(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0, 2], ascending=[False, True]) \
            .transform(dimx2_date_str_df, mock_dataset,
                       [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index() \
            .sort_values(['Timestamp', 'Wins'], ascending=[False, True]) \
            .set_index(['Timestamp', 'Party'])
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_empty_sort_array_is_ignored(self):
        result = Pandas(mock_dataset.fields.wins, sort=[]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_sort_value_greater_than_number_of_columns_is_ignored(self):
        result = Pandas(mock_dataset.fields.wins, sort=[5]) \
            .transform(dimx1_date_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_sort_with_no_index(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0]) \
            .transform(no_index_df, mock_dataset, [mock_dataset.fields.timestamp], [])

        expected = no_index_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(_format_float)

        pandas.testing.assert_frame_equal(expected, result)
