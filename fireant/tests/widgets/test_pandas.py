import copy
from functools import partial
from unittest import TestCase

import numpy as np
import pandas as pd
import pandas.testing
from pypika import Table
from pypika.analytics import Sum

from fireant import DataSet, DataType, Field, Rollup
from fireant.tests.dataset.mocks import (
    CumSum,
    ElectionOverElection,
    dimx0_metricx1_df,
    dimx0_metricx2_df,
    dimx1_date_df,
    dimx1_date_operation_df,
    dimx1_num_df,
    dimx1_str_df,
    dimx2_date_str_df,
    dimx2_date_str_ref_df,
    mock_dataset,
    no_index_df,
    test_database,
)
from fireant.utils import alias_selector as f
from fireant.widgets.pandas import Pandas


def format_float(x, is_raw=False):
    if pd.isnull(x):
        return ''
    if x in [np.inf, -np.inf]:
        return 'Inf'
    return f'{x:.0f}' if is_raw else f'{x:,.0f}'


format_float_raw = partial(format_float, is_raw=True)


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PandasTransformerTests(TestCase):
    maxDiff = None

    def test_metricx1(self):
        result = Pandas(mock_dataset.fields.votes).transform(dimx0_metricx1_df, [], [])

        expected = dimx0_metricx1_df.copy()[[f('votes')]]
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2(self):
        result = Pandas(mock_dataset.fields.votes, mock_dataset.fields.wins).transform(dimx0_metricx2_df, [], [])

        expected = dimx0_metricx2_df.copy()[[f('votes'), f('wins')]]
        expected.columns = ['Votes', 'Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_reversed(self):
        result = Pandas(mock_dataset.fields.wins, mock_dataset.fields.votes).transform(dimx0_metricx2_df, [], [])

        expected = dimx0_metricx2_df.copy()[[f('wins'), f('votes')]]
        expected.columns = ['Wins', 'Votes']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_date(self):
        result = Pandas(mock_dataset.fields.wins).transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_date_with_operation(self):
        result = Pandas(CumSum(mock_dataset.fields.votes)).transform(
            dimx1_date_operation_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_operation_df.copy()[[f('cumsum(votes)')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['CumSum(Votes)']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_str(self):
        result = Pandas(mock_dataset.fields.wins).transform(dimx1_str_df, [mock_dataset.fields.political_party], [])

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx1_int(self):
        result = Pandas(mock_dataset.fields.wins).transform(dimx1_num_df, [mock_dataset.fields['candidate-id']], [])

        expected = dimx1_num_df.copy()[[f('wins')]]
        expected.index.names = ['Candidate ID']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx2_date_str(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        result = Pandas(mock_dataset.fields.wins).transform(dimx2_date_str_df, dimensions, [])

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_transpose_dimx2_str(self):
        result = Pandas(mock_dataset.fields.wins, transpose=True).transform(
            dimx1_str_df, [mock_dataset.fields.political_party], []
        )

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.transpose()
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx1_str_transposes_data_frame(self):
        result = Pandas(mock_dataset.fields.wins, pivot=[mock_dataset.fields.political_party]).transform(
            dimx1_str_df, [mock_dataset.fields.political_party], []
        )

        expected = dimx1_str_df.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.transpose()
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str(self):
        result = Pandas(mock_dataset.fields.wins, pivot=[mock_dataset.fields.political_party]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_hidden_dimx2_date_str(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        result = Pandas(mock_dataset.fields.wins, hide=[mock_dataset.fields.political_party]).transform(
            dimx2_date_str_df, dimensions, []
        )

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.reset_index('$political_party', inplace=True, drop=True)
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_hidden_metric_dimx2_date_str(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = Pandas(mock_dataset.fields.votes, hide=[mock_dataset.fields.votes]).transform(
            dimx2_date_str_ref_df, dimensions, references
        )

        expected = dimx2_date_str_ref_df.copy()[[f('votes_eoe')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Votes EoE']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_hidden_ref_dimx2_date_str(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = Pandas(mock_dataset.fields.votes, hide=['votes_eoe']).transform(
            dimx2_date_str_ref_df, dimensions, references
        )

        expected = dimx2_date_str_ref_df.copy()[[f('votes')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_fetch_only_dimx2_date_str(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        dimensions[1].fetch_only = True
        result = Pandas(mock_dataset.fields.wins).transform(dimx2_date_str_df, dimensions, [])
        dimensions[1].fetch_only = False

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.reset_index('$political_party', inplace=True, drop=True)
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_time_series_ref(self):
        dimensions = [mock_dataset.fields.timestamp, mock_dataset.fields.political_party]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = Pandas(mock_dataset.fields.votes).transform(dimx2_date_str_ref_df, dimensions, references)

        expected = dimx2_date_str_ref_df.copy()[[f('votes'), f('votes_eoe')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Votes', 'Votes EoE']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metric_format(self):
        import copy

        votes = copy.copy(mock_dataset.fields.votes)
        votes.prefix = '$'
        votes.suffix = '€'
        votes.precision = 2

        # divide the data frame by 3 to get a repeating decimal so we can check precision
        result = Pandas(votes).transform(dimx1_date_df / 3, [mock_dataset.fields.timestamp], [])

        f_votes = f('votes')
        expected = dimx1_date_df.copy()[[f_votes]]
        expected[f_votes] = ['${0:,.2f}€'.format(x) for x in expected[f_votes] / 3]
        expected.index.names = ['Timestamp']
        expected.columns = ['Votes']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)

    def test_nan_in_metrics(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.nan

        result = Pandas(mock_dataset.fields.wins).transform(
            cat_dim_df_with_nan, [mock_dataset.fields.political_party], []
        )

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_inf_in_metrics(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        result = Pandas(mock_dataset.fields.wins).transform(
            cat_dim_df_with_nan, [mock_dataset.fields.political_party], []
        )

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_neginf_in_metrics(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        result = Pandas(mock_dataset.fields.wins).transform(
            cat_dim_df_with_nan, [mock_dataset.fields.political_party], []
        )

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_inf_in_metrics_with_precision_zero(self):
        cat_dim_df_with_nan = dimx1_str_df.copy()
        cat_dim_df_with_nan['$wins'] = cat_dim_df_with_nan['$wins'].apply(float)
        cat_dim_df_with_nan.iloc[2, 1] = np.inf

        mock_modified_dataset = copy.deepcopy(mock_dataset)
        mock_modified_dataset.fields.wins.precision = 0

        result = Pandas(mock_modified_dataset.fields.wins).transform(
            cat_dim_df_with_nan, [mock_modified_dataset.fields.political_party], []
        )

        expected = cat_dim_df_with_nan.copy()[[f('wins')]]
        expected.index = pd.Index(['Democrat', 'Independent', 'Republican'], name='Party')
        expected['$wins'] = ['6', '0', 'Inf']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        pandas.testing.assert_frame_equal(expected, result)


class PandasTransformerSortTests(TestCase):
    def test_metricx2_sort_index_asc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0]).transform(
            dimx1_date_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_index()
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_index_desc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0], ascending=[False]).transform(
            dimx1_date_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_index(ascending=False)
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_value_asc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[1]).transform(
            dimx1_date_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_values(['Wins'])
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_value_desc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[1], ascending=[False]).transform(
            dimx1_date_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.sort_values(['Wins'], ascending=False)
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_metricx2_sort_index_and_value(self):
        result = Pandas(mock_dataset.fields.wins, sort=[-0, 1]).transform(
            dimx1_date_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = (
            expected.reset_index().sort_values(['Timestamp', 'Wins'], ascending=[True, False]).set_index('Timestamp')
        )
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_index_asc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.sort_index()
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_index_desc(self):
        result = Pandas(
            mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0], ascending=[False]
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.sort_index(ascending=False)
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_first_metric_asc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[1]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index().sort_values(['Democrat']).set_index('Timestamp')
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_metric_desc(self):
        result = Pandas(
            mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[1], ascending=[False]
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index().sort_values(['Democrat'], ascending=False).set_index('Timestamp')
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_metric_asc(self):
        result = Pandas(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[1]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index().sort_values(['Democrat'], ascending=True).set_index('Timestamp')
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx1_metricx2(self):
        result = Pandas(
            mock_dataset.fields.votes, mock_dataset.fields.wins, pivot=[mock_dataset.fields.timestamp]
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes'), f('wins')]]
        expected = expected.unstack(level=0)
        expected.index.names = ['Party']
        expected.columns = pd.MultiIndex.from_product(
            [
                ['Votes', 'Wins'],
                pd.DatetimeIndex(['1996-01-01', '2000-01-01', '2004-01-01', '2008-01-01', '2012-01-01', '2016-01-01']),
            ],
            names=['Metrics', 'Timestamp'],
        )

        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_second_metric_desc(self):
        result = Pandas(
            mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=1, ascending=False
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index().sort_values(['Democrat'], ascending=False).set_index('Timestamp')
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_with_sort_index_and_columns(self):
        result = Pandas(
            mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0, 2], ascending=[True, False]
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = (
            expected.reset_index()
            .sort_values(['Timestamp', 'Democrat'], ascending=[True, False])
            .set_index('Timestamp')
        )
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_use_first_value_for_ascending_when_arg_has_invalid_length(self):
        result = Pandas(
            mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0, 2], ascending=[True]
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index().sort_values(['Timestamp', 'Democrat'], ascending=True).set_index('Timestamp')
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_use_pandas_default_for_ascending_when_arg_empty_list(self):
        result = Pandas(
            mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party], sort=[0, 2], ascending=[]
        ).transform(dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], [])

        expected = dimx2_date_str_df.copy()[[f('votes')]]
        expected = expected.unstack(level=[1])
        expected.index.names = ['Timestamp']
        expected.columns = ['Democrat', 'Independent', 'Republican']
        expected.columns.names = ['Party']

        expected = expected.reset_index().sort_values(['Timestamp', 'Democrat'], ascending=None).set_index('Timestamp')
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx2_date_str_sort_index_level_0_default_ascending(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index().sort_values(['Timestamp']).set_index(['Timestamp', 'Party'])
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_dimx2_date_str_sort_index_level_0_asc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0], ascending=True).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index().sort_values(['Timestamp'], ascending=True).set_index(['Timestamp', 'Party'])
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_sort_index_level_1_desc(self):
        result = Pandas(mock_dataset.fields.wins, sort=[1], ascending=[False]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = expected.reset_index().sort_values(['Party'], ascending=[False]).set_index(['Timestamp', 'Party'])
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_dimx2_date_str_sort_index_and_values(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0, 2], ascending=[False, True]).transform(
            dimx2_date_str_df, [mock_dataset.fields.timestamp, mock_dataset.fields.political_party], []
        )

        expected = dimx2_date_str_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp', 'Party']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'

        expected = (
            expected.reset_index()
            .sort_values(['Timestamp', 'Wins'], ascending=[False, True])
            .set_index(['Timestamp', 'Party'])
        )
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_empty_sort_array_is_ignored(self):
        result = Pandas(mock_dataset.fields.wins, sort=[]).transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_sort_value_greater_than_number_of_columns_is_ignored(self):
        result = Pandas(mock_dataset.fields.wins, sort=[5]).transform(
            dimx1_date_df, [mock_dataset.fields.timestamp], []
        )

        expected = dimx1_date_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_sort_with_no_index(self):
        result = Pandas(mock_dataset.fields.wins, sort=[0]).transform(no_index_df, [mock_dataset.fields.timestamp], [])

        expected = no_index_df.copy()[[f('wins')]]
        expected.index.names = ['Timestamp']
        expected.columns = ['Wins']
        expected.columns.name = 'Metrics'
        expected = expected.applymap(format_float)

        pandas.testing.assert_frame_equal(expected, result)

    def test_pivoted_df_transformation_formats_totals_correctly(self):
        test_table = Table('test')

        ds = DataSet(
            table=test_table,
            database=test_database,
            fields=[
                Field('date', label='Date', definition=test_table.date, data_type=DataType.date),
                Field('locale', label='Locale', definition=test_table.locale, data_type=DataType.text),
                Field('company', label='Company', definition=test_table.text, data_type=DataType.text),
                Field('metric1', label='Metric1', definition=Sum(test_table.number), data_type=DataType.number),
                Field('metric2', label='Metric2', definition=Sum(test_table.number), data_type=DataType.number),
            ],
        )

        df = pd.DataFrame.from_dict(
            {
                '$metric1': {('~~totals', '~~totals'): 3, ('za', '~~totals'): 3, ('za', 'C1'): 2, ('za', 'C2'): 1},
                '$metric2': {('~~totals', '~~totals'): 4, ('za', '~~totals'): 4, ('za', 'C1'): 2, ('za', 'C2'): 2},
            }
        )
        df.index.names = [f(ds.fields.locale.alias), f(ds.fields.company.alias)]

        result = Pandas(ds.fields.metric1, ds.fields.metric2, pivot=[ds.fields.company]).transform(
            df, [Rollup(ds.fields.locale), Rollup(ds.fields.company)], [], use_raw_values=True
        )

        self.assertEqual(['Metrics', 'Company'], list(result.columns.names))
        self.assertEqual(
            [
                ('Metric1', 'C1'),
                ('Metric1', 'C2'),
                ('Metric1', 'Totals'),
                ('Metric2', 'C1'),
                ('Metric2', 'C2'),
                ('Metric2', 'Totals'),
            ],
            result.columns.values.tolist(),
        )
        self.assertEqual(['Locale'], list(result.index.names))
        self.assertEqual(['za', 'Totals'], result.index.values.tolist())
        self.assertEqual([['2', '1', '3', '2', '2', '4'], ['', '', '3', '', '', '4']], result.values.tolist())
