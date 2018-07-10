from datetime import date
from unittest import TestCase
from unittest.mock import Mock

import pandas as pd

from fireant.slicer.widgets.datatables import (
    DataTablesJS,
    _format_metric_cell,
)
from fireant.tests.slicer.mocks import (
    CumSum,
    ElectionOverElection,
    cat_dim_df,
    cont_cat_dim_df,
    cont_dim_df,
    cont_dim_operation_df,
    cont_uni_dim_all_totals_df,
    cont_uni_dim_df,
    cont_uni_dim_ref_df,
    cont_uni_dim_totals_df,
    multi_metric_df,
    single_metric_df,
    slicer,
    uni_dim_df,
)
from fireant.utils import format_dimension_key as fd


class DataTablesTransformerTests(TestCase):
    maxDiff = None

    def test_single_metric(self):
        result = DataTablesJS(slicer.metrics.votes) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            'columns': [{
                'data': 'votes',
                'title': 'Votes',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'votes': {'value': 111674336, 'display': '111674336'}
            }],
        }, result)

    def test_multiple_metrics(self):
        result = DataTablesJS(slicer.metrics.votes, slicer.metrics.wins) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            'columns': [{
                'data': 'votes',
                'title': 'Votes',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'votes': {'value': 111674336, 'display': '111674336'},
                'wins': {'value': 12, 'display': '12'},
            }],
        }, result)

    def test_multiple_metrics_reversed(self):
        result = DataTablesJS(slicer.metrics.wins, slicer.metrics.votes) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            'columns': [{
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'votes',
                'title': 'Votes',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'wins': {'value': 12, 'display': '12'},
                'votes': {'value': 111674336, 'display': '111674336'},
            }],
        }, result)

    def test_time_series_dim(self):
        result = DataTablesJS(slicer.metrics.wins) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '2', 'value': 2}
            }],
        }, result)

    def test_time_series_dim_with_operation(self):
        result = DataTablesJS(CumSum(slicer.metrics.votes)) \
            .transform(cont_dim_operation_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'cumsum(votes)',
                'title': 'CumSum(Votes)',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'cumsum(votes)': {'display': '15220449', 'value': 15220449},
                'timestamp': {'value': '1996-01-01'}
            }, {
                'cumsum(votes)': {'display': '31882466', 'value': 31882466},
                'timestamp': {'value': '2000-01-01'}
            }, {
                'cumsum(votes)': {'display': '51497398', 'value': 51497398},
                'timestamp': {'value': '2004-01-01'}
            }, {
                'cumsum(votes)': {'display': '72791613', 'value': 72791613},
                'timestamp': {'value': '2008-01-01'}
            }, {
                'cumsum(votes)': {'display': '93363823', 'value': 93363823},
                'timestamp': {'value': '2012-01-01'}
            }, {
                'cumsum(votes)': {'display': '111674336', 'value': 111674336},
                'timestamp': {'value': '2016-01-01'}
            }],
        }, result)

    def test_cat_dim(self):
        result = DataTablesJS(slicer.metrics.wins) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            'columns': [{
                'data': 'political_party',
                'title': 'Party',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'political_party': {'display': 'Democrat', 'value': 'd'},
                'wins': {'display': '6', 'value': 6}
            }, {
                'political_party': {'display': 'Independent', 'value': 'i'},
                'wins': {'display': '0', 'value': 0}
            }, {
                'political_party': {'display': 'Republican', 'value': 'r'},
                'wins': {'display': '6', 'value': 6}
            }],
        }, result)

    def test_uni_dim(self):
        result = DataTablesJS(slicer.metrics.wins) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        self.assertEqual({
            'columns': [{
                'data': 'candidate',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'Candidate'
            }, {
                'data': 'wins',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'Wins'
            }],
            'data': [{
                'candidate': {'display': 'Bill Clinton', 'value': 1},
                'wins': {'display': '2', 'value': 2}
            }, {
                'candidate': {'display': 'Bob Dole', 'value': 2},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'display': 'Ross Perot', 'value': 3},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'display': 'George Bush', 'value': 4},
                'wins': {'display': '4', 'value': 4}
            }, {
                'candidate': {'display': 'Al Gore', 'value': 5},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'display': 'John Kerry', 'value': 6},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'display': 'Barrack Obama', 'value': 7},
                'wins': {'display': '4', 'value': 4}
            }, {
                'candidate': {'display': 'John McCain', 'value': 8},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'display': 'Mitt Romney', 'value': 9},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'display': 'Donald Trump', 'value': 10},
                'wins': {'display': '2', 'value': 2}
            }, {
                'candidate': {'display': 'Hillary Clinton', 'value': 11},
                'wins': {'display': '0', 'value': 0}
            }],
        }, result)

    def test_uni_dim_no_display_definition(self):
        import copy
        candidate = copy.copy(slicer.dimensions.candidate)
        candidate.display_key = None
        candidate.display_definition = None

        uni_dim_df_copy = uni_dim_df.copy()
        del uni_dim_df_copy[fd(slicer.dimensions.candidate.display_key)]

        result = DataTablesJS(slicer.metrics.wins) \
            .transform(uni_dim_df_copy, slicer, [candidate], [])

        self.assertEqual({
            'columns': [{
                'data': 'candidate',
                'render': {'_': 'value'},
                'title': 'Candidate'
            }, {
                'data': 'wins',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'Wins'
            }],
            'data': [{
                'candidate': {'value': 1},
                'wins': {'display': '2', 'value': 2}
            }, {
                'candidate': {'value': 2},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'value': 3},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'value': 4},
                'wins': {'display': '4', 'value': 4}
            }, {
                'candidate': {'value': 5},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'value': 6},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'value': 7},
                'wins': {'display': '4', 'value': 4}
            }, {
                'candidate': {'value': 8},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'value': 9},
                'wins': {'display': '0', 'value': 0}
            }, {
                'candidate': {'value': 10},
                'wins': {'display': '2', 'value': 2}
            }, {
                'candidate': {'value': 11},
                'wins': {'display': '0', 'value': 0}
            }],
        }, result)

    def test_multi_dims_time_series_and_uni(self):
        result = DataTablesJS(slicer.metrics.wins) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'state',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'State'
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'timestamp': {'value': '1996-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '1996-01-01'},
                'state': {'display': 'California', 'value': 2},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2000-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2000-01-01'},
                'state': {'display': 'California', 'value': 2},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2004-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2004-01-01'},
                'state': {'display': 'California', 'value': 2},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2008-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2008-01-01'},
                'state': {'display': 'California', 'value': 2},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2012-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2012-01-01'},
                'state': {'display': 'California', 'value': 2},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2016-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'wins': {'display': '1', 'value': 1}
            }, {
                'timestamp': {'value': '2016-01-01'},
                'state': {'display': 'California', 'value': 2},
                'wins': {'display': '1', 'value': 1}
            }],
        }, result)

    def test_multi_dims_with_one_level_totals(self):
        result = DataTablesJS(slicer.metrics.wins) \
            .transform(cont_uni_dim_totals_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state.rollup()],
                       [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'state',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'State'
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '2', 'value': 2}
            }],
        }, result)

    def test_multi_dims_with_all_levels_totals(self):
        result = DataTablesJS(slicer.metrics.wins) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            slicer.dimensions.state.rollup()], [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'state',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'State'
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '1996-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2000-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2004-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2008-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2012-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Texas', 'value': 1},
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'California', 'value': 2},
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '1', 'value': 1}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': '2016-01-01'},
                'wins': {'display': '2', 'value': 2}
            }, {
                'state': {'display': 'Totals', 'value': 'Totals'},
                'timestamp': {'value': 'Totals'},
                'wins': {'display': '12', 'value': 12}
            }],
        }, result)

    def test_pivoted_single_dimension_no_effect(self):
        result = DataTablesJS(slicer.metrics.wins, pivot=True) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            'columns': [{
                'data': 'political_party',
                'title': 'Party',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'wins',
                'title': 'Wins',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'political_party': {'display': 'Democrat', 'value': 'd'},
                'wins': {'display': '6', 'value': 6}
            }, {
                'political_party': {'display': 'Independent', 'value': 'i'},
                'wins': {'display': '0', 'value': 0}
            }, {
                'political_party': {'display': 'Republican', 'value': 'r'},
                'wins': {'display': '6', 'value': 6}
            }],
        }, result)

    def test_pivoted_multi_dims_time_series_and_cat(self):
        result = DataTablesJS(slicer.metrics.wins, pivot=True) \
            .transform(cont_cat_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.political_party], [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'wins.d',
                'title': 'Democrat',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'wins.i',
                'title': 'Independent',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'wins.r',
                'title': 'Republican',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'timestamp': {'value': '1996-01-01'},
                'wins': {
                    'd': {'display': '2', 'value': 2.0},
                    'i': {'display': '0', 'value': 0.0},
                    'r': {'display': '0', 'value': 0.0}
                }
            }, {
                'timestamp': {'value': '2000-01-01'},
                'wins': {
                    'd': {'display': '0', 'value': 0.0},
                    'i': {'display': '0', 'value': 0.0},
                    'r': {'display': '2', 'value': 2.0}
                }
            }, {
                'timestamp': {'value': '2004-01-01'},
                'wins': {
                    'd': {'display': '0', 'value': 0.0},
                    'i': {'display': '0', 'value': 0.0},
                    'r': {'display': '2', 'value': 2.0}
                }
            }, {
                'timestamp': {'value': '2008-01-01'},
                'wins': {
                    'd': {'display': '2', 'value': 2.0},
                    'i': {'display': '0', 'value': 0.0},
                    'r': {'display': '0', 'value': 0.0}
                }
            }, {
                'timestamp': {'value': '2012-01-01'},
                'wins': {
                    'd': {'display': '2', 'value': 2.0},
                    'i': {'display': '0', 'value': 0.0},
                    'r': {'display': '0', 'value': 0.0}
                }
            }, {
                'timestamp': {'value': '2016-01-01'},
                'wins': {
                    'd': {'display': '0', 'value': 0.0},
                    'i': {'display': '0', 'value': 0.0},
                    'r': {'display': '2', 'value': 2.0}
                }
            }],
        }, result)

    def test_pivoted_multi_dims_time_series_and_uni(self):
        result = DataTablesJS(slicer.metrics.votes, pivot=True) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'votes.1',
                'title': 'Texas',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'votes.2',
                'title': 'California',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'timestamp': {'value': '1996-01-01'},
                'votes': {
                    '1': {'display': '5574387', 'value': 5574387},
                    '2': {'display': '9646062', 'value': 9646062}
                }
            }, {
                'timestamp': {'value': '2000-01-01'},
                'votes': {
                    '1': {'display': '6233385', 'value': 6233385},
                    '2': {'display': '10428632', 'value': 10428632}
                }
            }, {
                'timestamp': {'value': '2004-01-01'},
                'votes': {
                    '1': {'display': '7359621', 'value': 7359621},
                    '2': {'display': '12255311', 'value': 12255311}
                }
            }, {
                'timestamp': {'value': '2008-01-01'},
                'votes': {
                    '1': {'display': '8007961', 'value': 8007961},
                    '2': {'display': '13286254', 'value': 13286254}
                }
            }, {
                'timestamp': {'value': '2012-01-01'},
                'votes': {
                    '1': {'display': '7877967', 'value': 7877967},
                    '2': {'display': '12694243', 'value': 12694243}
                }
            }, {
                'timestamp': {'value': '2016-01-01'},
                'votes': {
                    '1': {'display': '5072915', 'value': 5072915},
                    '2': {'display': '13237598', 'value': 13237598}
                }
            }],
        }, result)

    def test_time_series_ref(self):
        result = DataTablesJS(slicer.metrics.votes) \
            .transform(cont_uni_dim_ref_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp)
                       ])

        self.assertEqual({
            'columns': [{
                'data': 'timestamp',
                'title': 'Timestamp',
                'render': {'_': 'value'},
            }, {
                'data': 'state',
                'render': {'_': 'value', 'display': 'display'},
                'title': 'State'
            }, {
                'data': 'votes',
                'title': 'Votes',
                'render': {'_': 'value', 'display': 'display'},
            }, {
                'data': 'votes_eoe',
                'title': 'Votes (EoE)',
                'render': {'_': 'value', 'display': 'display'},
            }],
            'data': [{
                'timestamp': {'value': '2000-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'votes': {'display': '6233385', 'value': 6233385.},
                'votes_eoe': {'display': '5574387', 'value': 5574387.},
            }, {
                'timestamp': {'value': '2000-01-01'},
                'state': {'display': 'California', 'value': 2},
                'votes': {'display': '10428632', 'value': 10428632.},
                'votes_eoe': {'display': '9646062', 'value': 9646062.},
            }, {
                'timestamp': {'value': '2004-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'votes': {'display': '7359621', 'value': 7359621.},
                'votes_eoe': {'display': '6233385', 'value': 6233385.},
            }, {
                'timestamp': {'value': '2004-01-01'},
                'state': {'display': 'California', 'value': 2},
                'votes': {'display': '12255311', 'value': 12255311.},
                'votes_eoe': {'display': '10428632', 'value': 10428632.},
            }, {
                'timestamp': {'value': '2008-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'votes': {'display': '8007961', 'value': 8007961.},
                'votes_eoe': {'display': '7359621', 'value': 7359621.},
            }, {
                'timestamp': {'value': '2008-01-01'},
                'state': {'display': 'California', 'value': 2},
                'votes': {'display': '13286254', 'value': 13286254.},
                'votes_eoe': {'display': '12255311', 'value': 12255311.},
            }, {
                'timestamp': {'value': '2012-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'votes': {'display': '7877967', 'value': 7877967.},
                'votes_eoe': {'display': '8007961', 'value': 8007961.},
            }, {
                'timestamp': {'value': '2012-01-01'},
                'state': {'display': 'California', 'value': 2},
                'votes': {'display': '12694243', 'value': 12694243.},
                'votes_eoe': {'display': '13286254', 'value': 13286254.},
            }, {
                'timestamp': {'value': '2016-01-01'},
                'state': {'display': 'Texas', 'value': 1},
                'votes': {'display': '5072915', 'value': 5072915.},
                'votes_eoe': {'display': '7877967', 'value': 7877967.},
            }, {
                'timestamp': {'value': '2016-01-01'},
                'state': {'display': 'California', 'value': 2},
                'votes': {'display': '13237598', 'value': 13237598.},
                'votes_eoe': {'display': '12694243', 'value': 12694243.},
            }],
        }, result)


class MetricCellFormatTests(TestCase):
    def _mock_metric(self, prefix=None, suffix=None, precision=None):
        mock_metric = Mock()
        mock_metric.prefix = prefix
        mock_metric.suffix = suffix
        mock_metric.precision = precision
        return mock_metric

    def test_does_not_prettify_none_string(self):
        value = _format_metric_cell('None', self._mock_metric())
        self.assertDictEqual(value, {'value': 'None', 'display': 'None'})

    def test_does_prettify_non_none_strings(self):
        value = _format_metric_cell('abcde', self._mock_metric())
        self.assertDictEqual(value, {'value': 'abcde', 'display': 'abcde'})

    def test_does_prettify_int_values(self):
        value = _format_metric_cell(123, self._mock_metric())
        self.assertDictEqual(value, {'value': 123, 'display': '123'})

    def test_does_prettify_pandas_date_objects(self):
        value = _format_metric_cell(pd.Timestamp(date(2016, 5, 10)), self._mock_metric())
        self.assertDictEqual(value, {'value': '2016-05-10', 'display': '2016-05-10'})
