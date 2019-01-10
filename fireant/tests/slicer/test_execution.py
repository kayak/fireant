from unittest import (
    TestCase,
    skip,
)

import numpy as np
import pandas as pd
import pandas.testing

from fireant.slicer.queries.execution import reduce_result_set
from fireant.slicer.totals import get_totals_marker_for_dtype
from .mocks import (
    cat_dim_df,
    cat_dim_totals_df,
    cat_uni_dim_df,
    cont_cat_dim_all_totals_df,
    cont_cat_dim_df,
    cont_cat_dim_totals_df,
    cont_cat_uni_dim_all_totals_df,
    cont_cat_uni_dim_df,
    cont_dim_df,
    single_metric_df,
    slicer,
)

pd.set_option('display.expand_frame_repr', False)


def replace_totals(data_frame):
    index_names = data_frame.index.names

    raw = data_frame.reset_index()
    for name in index_names:
        marker = get_totals_marker_for_dtype(raw[name].dtype)
        raw[name].replace(marker, np.nan, inplace=True)

    return raw


class ReduceResultSetsTests(TestCase):
    def test_reduce_single_result_set_no_dimensions(self):
        expected = single_metric_df
        raw_df = expected

        dimensions = ()
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_dimension(self):
        expected = cont_dim_df
        raw_df = replace_totals(expected)

        dimensions = (slicer.dimensions.timestamp,)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cat_dimension(self):
        expected = cat_dim_df
        raw_df = replace_totals(expected)

        dimensions = (slicer.dimensions.political_party,)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_dimensions(self):
        expected = cont_cat_dim_df
        raw_df = replace_totals(expected)

        dimensions = (slicer.dimensions.timestamp, slicer.dimensions.political_party)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cat_uni_dimensions(self):
        expected = cat_uni_dim_df.sort_index()
        raw_df = replace_totals(expected)

        dimensions = (slicer.dimensions.political_party, slicer.dimensions.candidate)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_uni_dimensions(self):
        expected = cont_cat_uni_dim_df
        raw_df = replace_totals(expected)

        dimensions = (slicer.dimensions.timestamp, slicer.dimensions.political_party, slicer.dimensions.state)
        result = reduce_result_set([raw_df], (), dimensions, ())
        pandas.testing.assert_frame_equal(expected, result)


class ReduceResultSetsWithTotalsTests(TestCase):
    def test_reduce_single_result_set_with_cat_dimension(self):
        expected = cat_dim_totals_df
        raw_df = replace_totals(cat_dim_df)
        totals_df = pd.merge(pd.DataFrame([None], columns=['$d$political_party']),
                             pd.DataFrame([raw_df[['$m$votes', '$m$wins']].sum(axis=0)]),
                             how='outer',
                             left_index=True,
                             right_index=True)

        dimensions = (slicer.dimensions.political_party.rollup(),)
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_dimensions_cont_totals(self):
        expected = cont_cat_dim_all_totals_df.loc[(slice(None), slice('d', 'r')), :] \
            .append(cont_cat_dim_all_totals_df.iloc[-1])
        raw_df = replace_totals(cont_cat_dim_df)
        totals_df = pd.merge(pd.DataFrame([[None, None]], columns=['$d$timestamp', '$d$political_party']),
                             pd.DataFrame([raw_df[['$m$votes', '$m$wins']].sum(axis=0)]),
                             how='outer',
                             left_index=True,
                             right_index=True)

        dimensions = (slicer.dimensions.timestamp.rollup(), slicer.dimensions.political_party)
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_dimensions_cat_totals(self):
        expected = cont_cat_dim_totals_df
        raw_df = replace_totals(cont_cat_dim_df)
        totals_df = raw_df.groupby('$d$timestamp').sum().reset_index()
        totals_df['$d$political_party'] = None
        totals_df = totals_df[['$d$timestamp', '$d$political_party', '$m$votes', '$m$wins']]

        dimensions = (slicer.dimensions.timestamp, slicer.dimensions.political_party.rollup())
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_uni_dimensions_cont_totals(self):
        expected = cont_cat_uni_dim_all_totals_df.loc[(slice(None), slice('d', 'r'), slice('1', '2')), :] \
            .append(cont_cat_uni_dim_all_totals_df.iloc[-1])
        raw_df = replace_totals(cont_cat_uni_dim_df)
        totals_df = pd.merge(pd.DataFrame([[None, None, None, None]],
                                          columns=['$d$timestamp', '$d$political_party',
                                                   '$d$state', '$d$state_display']),
                             pd.DataFrame([raw_df[['$m$votes', '$m$wins']].sum(axis=0)]),
                             how='outer',
                             left_index=True,
                             right_index=True)
        totals_df = totals_df[['$d$timestamp', '$d$political_party', '$d$state', '$d$state_display',
                               '$m$votes', '$m$wins']]

        dimensions = (slicer.dimensions.timestamp.rollup(), slicer.dimensions.political_party, slicer.dimensions.state)
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_uni_dimensions_cat_totals(self):
        expected = cont_cat_uni_dim_all_totals_df.loc[(slice(None), slice(None), slice('1', '2')), :] \
            .append(cont_cat_uni_dim_all_totals_df.loc[(slice(None), '~~totals'), :].iloc[:-1]) \
            .sort_index()
        raw_df = replace_totals(cont_cat_uni_dim_df)
        totals_df = raw_df.groupby('$d$timestamp').sum().reset_index()
        totals_df['$d$political_party'] = None
        totals_df['$d$state'] = None
        totals_df['$d$state_display'] = None
        totals_df = totals_df[['$d$timestamp', '$d$political_party', '$d$state', '$d$state_display',
                               '$m$votes', '$m$wins']]

        dimensions = (slicer.dimensions.timestamp, slicer.dimensions.political_party.rollup(), slicer.dimensions.state)
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    def test_reduce_single_result_set_with_cont_cat_uni_dimensions_uni_totals(self):
        expected = cont_cat_uni_dim_all_totals_df.loc[(slice(None), slice('d', 'r')), :]
        raw_df = replace_totals(cont_cat_uni_dim_df)
        totals_df = raw_df.groupby(['$d$timestamp', '$d$political_party']).sum().reset_index()
        totals_df['$d$state'] = None
        totals_df['$d$state_display'] = None
        totals_df = totals_df[['$d$timestamp', '$d$political_party', '$d$state', '$d$state_display',
                               '$m$votes', '$m$wins']]

        dimensions = (slicer.dimensions.timestamp, slicer.dimensions.political_party, slicer.dimensions.state.rollup())
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)

    @skip('BAN-2594')
    def test_reduce_single_result_set_with_cont_cat_uni_dimensions_cat_totals_with_null_in_cont_dim(self):
        index_names = list(cont_cat_uni_dim_all_totals_df.index.names)
        nulls = pd.DataFrame([[np.nan, 'd', '1', 'Texas', 5, 0], [np.nan, 'd', '2', 'California', 2, 0],
                              [np.nan, 'i', '1', 'Texas', 5, 0], [np.nan, 'i', '2', 'California', 7, 0],
                              [np.nan, 'r', '1', 'Texas', 11, 0], [np.nan, 'r', '2', 'California', 3, 0]],
                             columns=index_names + list(cont_cat_uni_dim_all_totals_df.columns))
        nulls_totals = pd.DataFrame([nulls[['$m$votes', '$m$wins']].sum()])
        nulls_totals[index_names[0]] = np.nan
        nulls_totals[index_names[1]] = '~~totals'
        nulls_totals[index_names[2]] = '~~totals'

        expected = cont_cat_uni_dim_all_totals_df.loc[(slice(None), slice(None), slice('1', '2')), :] \
            .append(cont_cat_uni_dim_all_totals_df.loc[(slice(None), '~~totals'), :].iloc[:-1]) \
            .append(nulls.set_index(index_names)) \
            .append(nulls_totals.set_index(index_names)) \
            .sort_index()
        raw_df = replace_totals(cont_cat_uni_dim_df)
        raw_df = nulls \
            .append(raw_df) \
            .sort_values(['$d$timestamp', '$d$political_party', '$d$state'])

        totals_df = raw_df.groupby('$d$timestamp').sum().reset_index()
        null_totals_df = pd.DataFrame([raw_df[raw_df['$d$timestamp'].isnull()]
                                       [['$m$votes', '$m$wins']].sum()])
        null_totals_df['$d$timestamp'] = None
        totals_df = totals_df.append(null_totals_df)
        totals_df['$d$political_party'] = None
        totals_df['$d$state'] = None
        totals_df['$d$state_display'] = None
        totals_df = totals_df[['$d$timestamp', '$d$political_party', '$d$state', '$d$state_display',
                               '$m$votes', '$m$wins']]

        dimensions = (slicer.dimensions.timestamp, slicer.dimensions.political_party.rollup(), slicer.dimensions.state)
        result = reduce_result_set([raw_df, totals_df], (), dimensions, ())

        pandas.testing.assert_frame_equal(expected, result)
