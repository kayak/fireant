from unittest import TestCase

from fireant.slicer.widgets.reacttable import (
    ReactTable,
    display_value,
)
from fireant.tests.slicer.mocks import (
    CumSum,
    ElectionOverElection,
    cat_dim_df,
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


class ReactTableTransformerTests(TestCase):
    maxDiff = None

    def test_single_metric(self):
        result = ReactTable(slicer.metrics.votes) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            'columns': [{'Header': 'Votes', 'accessor': '$m$votes'}],
            'data': [{'$m$votes': {'raw': 111674336}}]
        }, result)

    def test_multiple_metrics(self):
        result = ReactTable(slicer.metrics.votes, slicer.metrics.wins) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            'columns': [{'Header': 'Votes', 'accessor': '$m$votes'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{'$m$votes': {'raw': 111674336}, '$m$wins': {'raw': 12}}]
        }, result)

    def test_multiple_metrics_reversed(self):
        result = ReactTable(slicer.metrics.wins, slicer.metrics.votes) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            'columns': [{'Header': 'Wins', 'accessor': '$m$wins'},
                        {'Header': 'Votes', 'accessor': '$m$votes'}],
            'data': [{'$m$votes': {'raw': 111674336}, '$m$wins': {'raw': 12}}]
        }, result)

    def test_time_series_dim(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{'$d$timestamp': {'raw': '1996-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2000-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2004-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2008-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2012-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2016-01-01'}, '$m$wins': {'raw': 2}}]
        }, result)

    def test_time_series_dim_with_operation(self):
        result = ReactTable(CumSum(slicer.metrics.votes)) \
            .transform(cont_dim_operation_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'CumSum(Votes)', 'accessor': '$m$cumsum(votes)'}],
            'data': [{
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$cumsum(votes)': {'raw': 15220449}
            }, {
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$cumsum(votes)': {'raw': 31882466}
            }, {
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$cumsum(votes)': {'raw': 51497398}
            }, {
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$cumsum(votes)': {'raw': 72791613}
            }, {
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$cumsum(votes)': {'raw': 93363823}
            }, {
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$cumsum(votes)': {'raw': 111674336}
            }]
        }, result)

    def test_cat_dim(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            'columns': [{'Header': 'Party', 'accessor': '$d$political_party'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{
                '$d$political_party': {'display': 'Democrat', 'raw': 'd'},
                '$m$wins': {'raw': 6}
            }, {
                '$d$political_party': {'display': 'Independent', 'raw': 'i'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$political_party': {'display': 'Republican', 'raw': 'r'},
                '$m$wins': {'raw': 6}
            }]
        }, result)

    def test_uni_dim(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        self.assertEqual({
            'columns': [{'Header': 'Candidate', 'accessor': '$d$candidate'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{
                '$d$candidate': {'display': 'Bill Clinton', 'raw': '1'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$candidate': {'display': 'Bob Dole', 'raw': '2'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$candidate': {'display': 'Ross Perot', 'raw': '3'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$candidate': {'display': 'George Bush', 'raw': '4'},
                '$m$wins': {'raw': 4}
            }, {
                '$d$candidate': {'display': 'Al Gore', 'raw': '5'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$candidate': {'display': 'John Kerry', 'raw': '6'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$candidate': {'display': 'Barrack Obama', 'raw': '7'},
                '$m$wins': {'raw': 4}
            }, {
                '$d$candidate': {'display': 'John McCain', 'raw': '8'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$candidate': {'display': 'Mitt Romney', 'raw': '9'},
                '$m$wins': {'raw': 0}
            }, {
                '$d$candidate': {'display': 'Donald Trump', 'raw': '10'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$candidate': {'display': 'Hillary Clinton', 'raw': '11'},
                '$m$wins': {'raw': 0}
            }]
        }, result)

    def test_uni_dim_no_display_definition(self):
        import copy
        candidate = copy.copy(slicer.dimensions.candidate)
        candidate.display_key = None
        candidate.display_definition = None

        uni_dim_df_copy = uni_dim_df.copy()
        del uni_dim_df_copy[fd(slicer.dimensions.candidate.display_key)]

        result = ReactTable(slicer.metrics.wins) \
            .transform(uni_dim_df_copy, slicer, [candidate], [])

        self.assertEqual({
            'columns': [{'Header': 'Candidate', 'accessor': '$d$candidate'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{'$d$candidate': {'raw': '1'}, '$m$wins': {'raw': 2}},
                     {'$d$candidate': {'raw': '2'}, '$m$wins': {'raw': 0}},
                     {'$d$candidate': {'raw': '3'}, '$m$wins': {'raw': 0}},
                     {'$d$candidate': {'raw': '4'}, '$m$wins': {'raw': 4}},
                     {'$d$candidate': {'raw': '5'}, '$m$wins': {'raw': 0}},
                     {'$d$candidate': {'raw': '6'}, '$m$wins': {'raw': 0}},
                     {'$d$candidate': {'raw': '7'}, '$m$wins': {'raw': 4}},
                     {'$d$candidate': {'raw': '8'}, '$m$wins': {'raw': 0}},
                     {'$d$candidate': {'raw': '9'}, '$m$wins': {'raw': 0}},
                     {'$d$candidate': {'raw': '10'}, '$m$wins': {'raw': 2}},
                     {'$d$candidate': {'raw': '11'}, '$m$wins': {'raw': 0}}]
        }, result)

    def test_multi_dims_time_series_and_uni(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 1}
            }]
        }, result)

    def test_multi_dims_with_one_level_totals(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(cont_uni_dim_totals_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state.rollup()],
                       [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 2}
            }]
        }, result)

    def test_multi_dims_with_all_levels_totals(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            slicer.dimensions.state.rollup()], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 1}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$wins': {'raw': 2}
            }, {
                '$d$state': {'raw': 'Totals'},
                '$d$timestamp': {'raw': 'Totals'},
                '$m$wins': {'raw': 12}
            }]
        }, result)

    def test_time_series_ref(self):
        result = ReactTable(slicer.metrics.votes) \
            .transform(cont_uni_dim_ref_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp)
                       ])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': 'Votes', 'accessor': '$m$votes'},
                        {'Header': 'Votes (EoE)', 'accessor': '$m$votes_eoe'}],
            'data': [{
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {'raw': 6233385.0},
                '$m$votes_eoe': {'raw': 5574387.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {'raw': 10428632.0},
                '$m$votes_eoe': {'raw': 9646062.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {'raw': 7359621.0},
                '$m$votes_eoe': {'raw': 6233385.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {'raw': 12255311.0},
                '$m$votes_eoe': {'raw': 10428632.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {'raw': 8007961.0},
                '$m$votes_eoe': {'raw': 7359621.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {'raw': 13286254.0},
                '$m$votes_eoe': {'raw': 12255311.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {'raw': 7877967.0},
                '$m$votes_eoe': {'raw': 8007961.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {'raw': 12694243.0},
                '$m$votes_eoe': {'raw': 13286254.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {'raw': 5072915.0},
                '$m$votes_eoe': {'raw': 7877967.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {'raw': 13237598.0},
                '$m$votes_eoe': {'raw': 12694243.0}
            }]
        }, result)

    def test_time_series_ref_multiple_metrics(self):
        result = ReactTable(slicer.metrics.votes, slicer.metrics.wins) \
            .transform(cont_uni_dim_ref_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp)
                       ])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': 'Votes', 'accessor': '$m$votes'},
                        {'Header': 'Votes (EoE)', 'accessor': '$m$votes_eoe'},
                        {'Header': 'Wins', 'accessor': '$m$wins'},
                        {'Header': 'Wins (EoE)', 'accessor': '$m$wins_eoe'}],
            'data': [{
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {'raw': 6233385.0},
                '$m$votes_eoe': {'raw': 5574387.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {'raw': 10428632.0},
                '$m$votes_eoe': {'raw': 9646062.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {'raw': 7359621.0},
                '$m$votes_eoe': {'raw': 6233385.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {'raw': 12255311.0},
                '$m$votes_eoe': {'raw': 10428632.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {'raw': 8007961.0},
                '$m$votes_eoe': {'raw': 7359621.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {'raw': 13286254.0},
                '$m$votes_eoe': {'raw': 12255311.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {'raw': 7877967.0},
                '$m$votes_eoe': {'raw': 8007961.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {'raw': 12694243.0},
                '$m$votes_eoe': {'raw': 13286254.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {'raw': 5072915.0},
                '$m$votes_eoe': {'raw': 7877967.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }, {
                '$d$state': {'display': 'California', 'raw': '2'},
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {'raw': 13237598.0},
                '$m$votes_eoe': {'raw': 12694243.0},
                '$m$wins': {'raw': 1.0},
                '$m$wins_eoe': {'raw': 1.0}
            }]
        }, result)

    def test_transpose(self):
        result = ReactTable(slicer.metrics.wins, transpose=True) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            'columns': [{'Header': '', 'accessor': '$d$metrics'},
                        {'Header': 'Democrat', 'accessor': 'd'},
                        {'Header': 'Independent', 'accessor': 'i'},
                        {'Header': 'Republican', 'accessor': 'r'}],
            'data': [{
                '$d$metrics': {'raw': 'Wins'},
                'd': {'raw': 6},
                'i': {'raw': 0},
                'r': {'raw': 6}
            }]
        }, result)

    def test_pivot_second_dimension_with_one_metric(self):
        result = ReactTable(slicer.metrics.wins, pivot=[slicer.dimensions.state]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'Texas', 'accessor': '1'},
                        {'Header': 'California', 'accessor': '2'}],
            'data': [{
                '$d$timestamp': {'raw': '1996-01-01'},
                '1': {'raw': 1},
                '2': {'raw': 1}
            }, {
                '$d$timestamp': {'raw': '2000-01-01'},
                '1': {'raw': 1},
                '2': {'raw': 1}
            }, {
                '$d$timestamp': {'raw': '2004-01-01'},
                '1': {'raw': 1},
                '2': {'raw': 1}
            }, {
                '$d$timestamp': {'raw': '2008-01-01'},
                '1': {'raw': 1},
                '2': {'raw': 1}
            }, {
                '$d$timestamp': {'raw': '2012-01-01'},
                '1': {'raw': 1},
                '2': {'raw': 1}
            }, {
                '$d$timestamp': {'raw': '2016-01-01'},
                '1': {'raw': 1},
                '2': {'raw': 1}
            }]
        }, result)

    def test_pivot_second_dimension_with_multiple_metrics(self):
        result = ReactTable(slicer.metrics.wins, slicer.metrics.votes, pivot=[slicer.dimensions.state]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {
                            'Header': 'Votes',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$votes.1'},
                                        {'Header': 'California', 'accessor': '$m$votes.2'}]
                        }, {
                            'Header': 'Wins',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$wins.1'},
                                        {'Header': 'California', 'accessor': '$m$wins.2'}]
                        }],
            'data': [{
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$votes': {'1': {'raw': 5574387}, '2': {'raw': 9646062}},
                '$m$wins': {'1': {'raw': 1}, '2': {'raw': 1}}
            }, {
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {'1': {'raw': 6233385}, '2': {'raw': 10428632}},
                '$m$wins': {'1': {'raw': 1}, '2': {'raw': 1}}
            }, {
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {'1': {'raw': 7359621}, '2': {'raw': 12255311}},
                '$m$wins': {'1': {'raw': 1}, '2': {'raw': 1}}
            }, {
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {'1': {'raw': 8007961}, '2': {'raw': 13286254}},
                '$m$wins': {'1': {'raw': 1}, '2': {'raw': 1}}
            }, {
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {'1': {'raw': 7877967}, '2': {'raw': 12694243}},
                '$m$wins': {'1': {'raw': 1}, '2': {'raw': 1}}
            }, {
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {'1': {'raw': 5072915}, '2': {'raw': 13237598}},
                '$m$wins': {'1': {'raw': 1}, '2': {'raw': 1}}
            }]
        }, result)

    def test_pivot_second_dimension_with_multiple_metrics_and_references(self):
        result = ReactTable(slicer.metrics.votes, slicer.metrics.wins, pivot=[slicer.dimensions.state]) \
            .transform(cont_uni_dim_ref_df,
                       slicer, [
                           slicer.dimensions.timestamp, slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp)
                       ])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {
                            'Header': 'Votes',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$votes.1'},
                                        {'Header': 'California', 'accessor': '$m$votes.2'}]
                        }, {
                            'Header': 'Votes (EoE)',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$votes_eoe.1'},
                                        {
                                            'Header': 'California',
                                            'accessor': '$m$votes_eoe.2'
                                        }]
                        }, {
                            'Header': 'Wins',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$wins.1'},
                                        {'Header': 'California', 'accessor': '$m$wins.2'}]
                        }, {
                            'Header': 'Wins (EoE)',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$wins_eoe.1'},
                                        {
                                            'Header': 'California',
                                            'accessor': '$m$wins_eoe.2'
                                        }]
                        }],
            'data': [{
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {'1': {'raw': 6233385.0}, '2': {'raw': 10428632.0}},
                '$m$votes_eoe': {'1': {'raw': 5574387.0}, '2': {'raw': 9646062.0}},
                '$m$wins': {'1': {'raw': 1.0}, '2': {'raw': 1.0}},
                '$m$wins_eoe': {'1': {'raw': 1.0}, '2': {'raw': 1.0}}
            }, {
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {'1': {'raw': 7359621.0}, '2': {'raw': 12255311.0}},
                '$m$votes_eoe': {'1': {'raw': 6233385.0}, '2': {'raw': 10428632.0}},
                '$m$wins': {'1': {'raw': 1.0}, '2': {'raw': 1.0}},
                '$m$wins_eoe': {'1': {'raw': 1.0}, '2': {'raw': 1.0}}
            }, {
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {'1': {'raw': 8007961.0}, '2': {'raw': 13286254.0}},
                '$m$votes_eoe': {'1': {'raw': 7359621.0}, '2': {'raw': 12255311.0}},
                '$m$wins': {'1': {'raw': 1.0}, '2': {'raw': 1.0}},
                '$m$wins_eoe': {'1': {'raw': 1.0}, '2': {'raw': 1.0}}
            }, {
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {'1': {'raw': 7877967.0}, '2': {'raw': 12694243.0}},
                '$m$votes_eoe': {'1': {'raw': 8007961.0}, '2': {'raw': 13286254.0}},
                '$m$wins': {'1': {'raw': 1.0}, '2': {'raw': 1.0}},
                '$m$wins_eoe': {'1': {'raw': 1.0}, '2': {'raw': 1.0}}
            }, {
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {'1': {'raw': 5072915.0}, '2': {'raw': 13237598.0}},
                '$m$votes_eoe': {'1': {'raw': 7877967.0}, '2': {'raw': 12694243.0}},
                '$m$wins': {'1': {'raw': 1.0}, '2': {'raw': 1.0}},
                '$m$wins_eoe': {'1': {'raw': 1.0}, '2': {'raw': 1.0}}
            }]
        }, result)

    def test_pivot_single_dimension_as_rows_single_metric_metrics_automatically_pivoted(self):
        result = ReactTable(slicer.metrics.wins, pivot=[slicer.dimensions.candidate]) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        self.assertEqual({
            'columns': [{'Header': '', 'accessor': '$d$metrics'},
                        {'Header': 'Bill Clinton', 'accessor': '1'},
                        {'Header': 'Bob Dole', 'accessor': '2'},
                        {'Header': 'Ross Perot', 'accessor': '3'},
                        {'Header': 'George Bush', 'accessor': '4'},
                        {'Header': 'Al Gore', 'accessor': '5'},
                        {'Header': 'John Kerry', 'accessor': '6'},
                        {'Header': 'Barrack Obama', 'accessor': '7'},
                        {'Header': 'John McCain', 'accessor': '8'},
                        {'Header': 'Mitt Romney', 'accessor': '9'},
                        {'Header': 'Donald Trump', 'accessor': '10'},
                        {'Header': 'Hillary Clinton', 'accessor': '11'}],
            'data': [{
                '$d$metrics': {'raw': 'Wins'},
                '1': {'raw': 2},
                '10': {'raw': 2},
                '11': {'raw': 0},
                '2': {'raw': 0},
                '3': {'raw': 0},
                '4': {'raw': 4},
                '5': {'raw': 0},
                '6': {'raw': 0},
                '7': {'raw': 4},
                '8': {'raw': 0},
                '9': {'raw': 0}
            }]
        }, result)

    def test_pivot_single_dimension_as_rows_single_metric_and_transpose_set_to_true(self):
        result = ReactTable(slicer.metrics.wins, pivot=[slicer.dimensions.candidate], transpose=True) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        self.assertEqual({
            'columns': [{'Header': '', 'accessor': '$d$metrics'},
                        {'Header': 'Bill Clinton', 'accessor': '1'},
                        {'Header': 'Bob Dole', 'accessor': '2'},
                        {'Header': 'Ross Perot', 'accessor': '3'},
                        {'Header': 'George Bush', 'accessor': '4'},
                        {'Header': 'Al Gore', 'accessor': '5'},
                        {'Header': 'John Kerry', 'accessor': '6'},
                        {'Header': 'Barrack Obama', 'accessor': '7'},
                        {'Header': 'John McCain', 'accessor': '8'},
                        {'Header': 'Mitt Romney', 'accessor': '9'},
                        {'Header': 'Donald Trump', 'accessor': '10'},
                        {'Header': 'Hillary Clinton', 'accessor': '11'}],
            'data': [{
                '$d$metrics': {'raw': 'Wins'},
                '1': {'raw': 2},
                '10': {'raw': 2},
                '11': {'raw': 0},
                '2': {'raw': 0},
                '3': {'raw': 0},
                '4': {'raw': 4},
                '5': {'raw': 0},
                '6': {'raw': 0},
                '7': {'raw': 4},
                '8': {'raw': 0},
                '9': {'raw': 0}
            }]
        }, result)

    def test_pivot_single_dimension_as_rows_multiple_metrics(self):
        result = ReactTable(slicer.metrics.wins, slicer.metrics.votes,
                            pivot=[slicer.dimensions.candidate]) \
            .transform(uni_dim_df, slicer, [slicer.dimensions.candidate], [])

        self.assertEqual({
            'columns': [{'Header': '', 'accessor': '$d$metrics'},
                        {'Header': 'Bill Clinton', 'accessor': '1'},
                        {'Header': 'Bob Dole', 'accessor': '2'},
                        {'Header': 'Ross Perot', 'accessor': '3'},
                        {'Header': 'George Bush', 'accessor': '4'},
                        {'Header': 'Al Gore', 'accessor': '5'},
                        {'Header': 'John Kerry', 'accessor': '6'},
                        {'Header': 'Barrack Obama', 'accessor': '7'},
                        {'Header': 'John McCain', 'accessor': '8'},
                        {'Header': 'Mitt Romney', 'accessor': '9'},
                        {'Header': 'Donald Trump', 'accessor': '10'},
                        {'Header': 'Hillary Clinton', 'accessor': '11'}],
            'data': [{
                '$d$metrics': {'raw': 'Wins'},
                '1': {'raw': 2},
                '10': {'raw': 2},
                '11': {'raw': 0},
                '2': {'raw': 0},
                '3': {'raw': 0},
                '4': {'raw': 4},
                '5': {'raw': 0},
                '6': {'raw': 0},
                '7': {'raw': 4},
                '8': {'raw': 0},
                '9': {'raw': 0}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '1': {'raw': 7579518},
                '10': {'raw': 13438835},
                '11': {'raw': 4871678},
                '2': {'raw': 6564547},
                '3': {'raw': 1076384},
                '4': {'raw': 18403811},
                '5': {'raw': 8294949},
                '6': {'raw': 9578189},
                '7': {'raw': 24227234},
                '8': {'raw': 9491109},
                '9': {'raw': 8148082}
            }]
        }, result)

    def test_pivot_single_metric_time_series_dim(self):
        result = ReactTable(slicer.metrics.wins) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {'Header': 'Wins', 'accessor': '$m$wins'}],
            'data': [{'$d$timestamp': {'raw': '1996-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2000-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2004-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2008-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2012-01-01'}, '$m$wins': {'raw': 2}},
                     {'$d$timestamp': {'raw': '2016-01-01'}, '$m$wins': {'raw': 2}}]
        }, result)

    def test_pivot_multi_dims_with_all_levels_totals(self):
        state = slicer.dimensions.state.rollup()
        result = ReactTable(slicer.metrics.wins, slicer.metrics.votes, pivot=[state]) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            state], [])

        self.assertEqual({
            'columns': [{'Header': 'Timestamp', 'accessor': '$d$timestamp'},
                        {
                            'Header': 'Votes',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$votes.1'},
                                        {'Header': 'California', 'accessor': '$m$votes.2'},
                                        {
                                            'Header': 'Totals',
                                            'accessor': '$m$votes.totals',
                                            'className': 'fireant-totals'
                                        }]
                        }, {
                            'Header': 'Wins',
                            'columns': [{'Header': 'Texas', 'accessor': '$m$wins.1'},
                                        {'Header': 'California', 'accessor': '$m$wins.2'},
                                        {
                                            'Header': 'Totals',
                                            'accessor': '$m$wins.totals',
                                            'className': 'fireant-totals'
                                        }]
                        }],
            'data': [{
                '$d$timestamp': {'raw': '1996-01-01'},
                '$m$votes': {
                    '1': {'raw': 5574387.0},
                    '2': {'raw': 9646062.0},
                    'totals': {'raw': 15220449.0}
                },
                '$m$wins': {
                    '1': {'raw': 1.0},
                    '2': {'raw': 1.0},
                    'totals': {'raw': 2.0}
                }
            }, {
                '$d$timestamp': {'raw': '2000-01-01'},
                '$m$votes': {
                    '1': {'raw': 6233385.0},
                    '2': {'raw': 10428632.0},
                    'totals': {'raw': 16662017.0}
                },
                '$m$wins': {
                    '1': {'raw': 1.0},
                    '2': {'raw': 1.0},
                    'totals': {'raw': 2.0}
                }
            }, {
                '$d$timestamp': {'raw': '2004-01-01'},
                '$m$votes': {
                    '1': {'raw': 7359621.0},
                    '2': {'raw': 12255311.0},
                    'totals': {'raw': 19614932.0}
                },
                '$m$wins': {
                    '1': {'raw': 1.0},
                    '2': {'raw': 1.0},
                    'totals': {'raw': 2.0}
                }
            }, {
                '$d$timestamp': {'raw': '2008-01-01'},
                '$m$votes': {
                    '1': {'raw': 8007961.0},
                    '2': {'raw': 13286254.0},
                    'totals': {'raw': 21294215.0}
                },
                '$m$wins': {
                    '1': {'raw': 1.0},
                    '2': {'raw': 1.0},
                    'totals': {'raw': 2.0}
                }
            }, {
                '$d$timestamp': {'raw': '2012-01-01'},
                '$m$votes': {
                    '1': {'raw': 7877967.0},
                    '2': {'raw': 12694243.0},
                    'totals': {'raw': 20572210.0}
                },
                '$m$wins': {
                    '1': {'raw': 1.0},
                    '2': {'raw': 1.0},
                    'totals': {'raw': 2.0}
                }
            }, {
                '$d$timestamp': {'raw': '2016-01-01'},
                '$m$votes': {
                    '1': {'raw': 5072915.0},
                    '2': {'raw': 13237598.0},
                    'totals': {'raw': 18310513.0}
                },
                '$m$wins': {
                    '1': {'raw': 1.0},
                    '2': {'raw': 1.0},
                    'totals': {'raw': 2.0}
                }
            }, {
                '$d$timestamp': {'raw': 'Totals'},
                '$m$votes': {
                    '1': {'raw': ''},
                    '2': {'raw': ''},
                    'totals': {'raw': 111674336.0}
                },
                '$m$wins': {
                    '1': {'raw': ''},
                    '2': {'raw': ''},
                    'totals': {'raw': 12.0}
                }
            }]
        }, result)

    def test_pivot_first_dimension_and_transpose_with_all_levels_totals(self):
        state = slicer.dimensions.state.rollup()
        result = ReactTable(slicer.metrics.wins, slicer.metrics.votes, pivot=[state], transpose=True) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            state], [])

        self.assertEqual({
            'columns': [{'Header': '', 'accessor': '$d$metrics'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': '1996-01-01', 'accessor': '1996-01-01'},
                        {'Header': '2000-01-01', 'accessor': '2000-01-01'},
                        {'Header': '2004-01-01', 'accessor': '2004-01-01'},
                        {'Header': '2008-01-01', 'accessor': '2008-01-01'},
                        {'Header': '2012-01-01', 'accessor': '2012-01-01'},
                        {'Header': '2016-01-01', 'accessor': '2016-01-01'},
                        {
                            'Header': 'Totals',
                            'accessor': 'totals',
                            'className': 'fireant-totals'
                        }],
            'data': [{
                '$d$metrics': {'raw': 'Wins'},
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '1996-01-01': {'raw': 1.0},
                '2000-01-01': {'raw': 1.0},
                '2004-01-01': {'raw': 1.0},
                '2008-01-01': {'raw': 1.0},
                '2012-01-01': {'raw': 1.0},
                '2016-01-01': {'raw': 1.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Wins'},
                '$d$state': {'display': 'California', 'raw': '2'},
                '1996-01-01': {'raw': 1.0},
                '2000-01-01': {'raw': 1.0},
                '2004-01-01': {'raw': 1.0},
                '2008-01-01': {'raw': 1.0},
                '2012-01-01': {'raw': 1.0},
                '2016-01-01': {'raw': 1.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Wins'},
                '$d$state': {'raw': 'Totals'},
                '1996-01-01': {'raw': 2.0},
                '2000-01-01': {'raw': 2.0},
                '2004-01-01': {'raw': 2.0},
                '2008-01-01': {'raw': 2.0},
                '2012-01-01': {'raw': 2.0},
                '2016-01-01': {'raw': 2.0},
                'totals': {'raw': 12.0}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '1996-01-01': {'raw': 5574387.0},
                '2000-01-01': {'raw': 6233385.0},
                '2004-01-01': {'raw': 7359621.0},
                '2008-01-01': {'raw': 8007961.0},
                '2012-01-01': {'raw': 7877967.0},
                '2016-01-01': {'raw': 5072915.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '$d$state': {'display': 'California', 'raw': '2'},
                '1996-01-01': {'raw': 9646062.0},
                '2000-01-01': {'raw': 10428632.0},
                '2004-01-01': {'raw': 12255311.0},
                '2008-01-01': {'raw': 13286254.0},
                '2012-01-01': {'raw': 12694243.0},
                '2016-01-01': {'raw': 13237598.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '$d$state': {'raw': 'Totals'},
                '1996-01-01': {'raw': 15220449.0},
                '2000-01-01': {'raw': 16662017.0},
                '2004-01-01': {'raw': 19614932.0},
                '2008-01-01': {'raw': 21294215.0},
                '2012-01-01': {'raw': 20572210.0},
                '2016-01-01': {'raw': 18310513.0},
                'totals': {'raw': 111674336.0}
            }]
        }, result)

    def test_pivot_second_dimension_and_transpose_with_all_levels_totals(self):
        state = slicer.dimensions.state.rollup()
        result = ReactTable(slicer.metrics.wins, slicer.metrics.votes, pivot=[state], transpose=True) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            state], [])

        self.assertEqual({
            'columns': [{'Header': '', 'accessor': '$d$metrics'},
                        {'Header': 'State', 'accessor': '$d$state'},
                        {'Header': '1996-01-01', 'accessor': '1996-01-01'},
                        {'Header': '2000-01-01', 'accessor': '2000-01-01'},
                        {'Header': '2004-01-01', 'accessor': '2004-01-01'},
                        {'Header': '2008-01-01', 'accessor': '2008-01-01'},
                        {'Header': '2012-01-01', 'accessor': '2012-01-01'},
                        {'Header': '2016-01-01', 'accessor': '2016-01-01'},
                        {
                            'Header': 'Totals',
                            'accessor': 'totals',
                            'className': 'fireant-totals'
                        }],
            'data': [{
                '$d$metrics': {'raw': 'Wins'},
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '1996-01-01': {'raw': 1.0},
                '2000-01-01': {'raw': 1.0},
                '2004-01-01': {'raw': 1.0},
                '2008-01-01': {'raw': 1.0},
                '2012-01-01': {'raw': 1.0},
                '2016-01-01': {'raw': 1.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Wins'},
                '$d$state': {'display': 'California', 'raw': '2'},
                '1996-01-01': {'raw': 1.0},
                '2000-01-01': {'raw': 1.0},
                '2004-01-01': {'raw': 1.0},
                '2008-01-01': {'raw': 1.0},
                '2012-01-01': {'raw': 1.0},
                '2016-01-01': {'raw': 1.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Wins'},
                '$d$state': {'raw': 'Totals'},
                '1996-01-01': {'raw': 2.0},
                '2000-01-01': {'raw': 2.0},
                '2004-01-01': {'raw': 2.0},
                '2008-01-01': {'raw': 2.0},
                '2012-01-01': {'raw': 2.0},
                '2016-01-01': {'raw': 2.0},
                'totals': {'raw': 12.0}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '$d$state': {'display': 'Texas', 'raw': '1'},
                '1996-01-01': {'raw': 5574387.0},
                '2000-01-01': {'raw': 6233385.0},
                '2004-01-01': {'raw': 7359621.0},
                '2008-01-01': {'raw': 8007961.0},
                '2012-01-01': {'raw': 7877967.0},
                '2016-01-01': {'raw': 5072915.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '$d$state': {'display': 'California', 'raw': '2'},
                '1996-01-01': {'raw': 9646062.0},
                '2000-01-01': {'raw': 10428632.0},
                '2004-01-01': {'raw': 12255311.0},
                '2008-01-01': {'raw': 13286254.0},
                '2012-01-01': {'raw': 12694243.0},
                '2016-01-01': {'raw': 13237598.0},
                'totals': {'raw': ''}
            }, {
                '$d$metrics': {'raw': 'Votes'},
                '$d$state': {'raw': 'Totals'},
                '1996-01-01': {'raw': 15220449.0},
                '2000-01-01': {'raw': 16662017.0},
                '2004-01-01': {'raw': 19614932.0},
                '2008-01-01': {'raw': 21294215.0},
                '2012-01-01': {'raw': 20572210.0},
                '2016-01-01': {'raw': 18310513.0},
                'totals': {'raw': 111674336.0}
            }]
        }, result)


class ReactTableDisplayValueFormat(TestCase):
    def test_str_value_no_formats(self):
        display = display_value('abcdef')
        self.assertEqual('abcdef', display)

    def test_bool_true_value_no_formats(self):
        display = display_value(True)
        self.assertEqual('true', display)

    def test_bool_false_value_no_formats(self):
        display = display_value(False)
        self.assertEqual('false', display)

    def test_int_value_no_formats(self):
        display = display_value(12345)
        self.assertEqual('12,345', display)

    def test_decimal_value_no_formats(self):
        display = display_value(12345.123456789)
        self.assertEqual('12345.123456789', display)

    def test_str_value_with_prefix(self):
        display = display_value('abcdef', prefix='$')
        self.assertEqual('$abcdef', display)

    def test_bool_true_value_with_prefix(self):
        display = display_value(True, prefix='$')
        self.assertEqual('$true', display)

    def test_bool_false_value_with_prefix(self):
        display = display_value(False, prefix='$')
        self.assertEqual('$false', display)

    def test_int_value_with_prefix(self):
        display = display_value(12345, prefix='$')
        self.assertEqual('$12,345', display)

    def test_decimal_value_with_prefix(self):
        display = display_value(12345.123456789, prefix='$')
        self.assertEqual('$12345.123456789', display)

    def test_str_value_with_suffix(self):
        display = display_value('abcdef', suffix='€')
        self.assertEqual('abcdef€', display)

    def test_bool_true_value_with_suffix(self):
        display = display_value(True, suffix='€')
        self.assertEqual('true€', display)

    def test_bool_false_value_with_suffix(self):
        display = display_value(False, suffix='€')
        self.assertEqual('false€', display)

    def test_int_value_with_suffix(self):
        display = display_value(12345, suffix='€')
        self.assertEqual('12,345€', display)

    def test_decimal_value_with_suffix(self):
        display = display_value(12345.123456789, suffix='€')
        self.assertEqual('12345.123456789€', display)

    def test_str_value_with_precision(self):
        display = display_value('abcdef', precision=2)
        self.assertEqual('abcdef', display)

    def test_bool_true_value_with_precision(self):
        display = display_value(True, precision=2)
        self.assertEqual('true', display)

    def test_bool_false_value_with_precision(self):
        display = display_value(False, precision=2)
        self.assertEqual('false', display)

    def test_int_value_with_precision(self):
        display = display_value(12345, precision=2)
        self.assertEqual('12,345', display)

    def test_decimal_value_with_precision_0(self):
        display = display_value(12345.123456789, precision=0)
        self.assertEqual('12345', display)

    def test_decimal_value_with_precision_2(self):
        display = display_value(12345.123456789, precision=2)
        self.assertEqual('12345.12', display)
