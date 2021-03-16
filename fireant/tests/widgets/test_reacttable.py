from unittest import TestCase

import pandas as pd
from pypika import Table

from fireant import (
    DataSet,
    DataType,
    DayOverDay,
    Field,
    Rollup,
    day,
)
from fireant.dataset.filters import ComparisonOperator
from fireant.tests.database.mock_database import TestDatabase
from fireant.tests.dataset.mocks import (
    CumSum,
    ElectionOverElection,
    dimx0_metricx1_df,
    dimx0_metricx2_df,
    dimx1_date_df,
    dimx1_date_operation_df,
    dimx1_none_df,
    dimx1_num_df,
    dimx1_str_df,
    dimx2_date_str_df,
    dimx2_date_str_ref_df,
    dimx2_date_str_totals_df,
    dimx2_date_str_totalsx2_df,
    dimx2_str_str_df,
    mock_dataset,
)
from fireant.widgets.base import ReferenceItem
from fireant.widgets.reacttable import (
    FormattingConditionRule,
    FormattingField,
    FormattingHeatMapRule,
    ReactTable,
)


class FormattingRulesTests(TestCase):
    @classmethod
    def setUpClass(cls):
        db = TestDatabase()
        t0 = Table('test0')
        cls.dataset = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    'timestamp',
                    label='Timestamp',
                    definition=t0.timestamp,
                    data_type=DataType.number,
                ),
                Field(
                    'metric0',
                    label='Metric0',
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )

        cls.df = pd.DataFrame.from_dict(
            {
                '$timestamp': [100, 200, 300, 400],
                '$metric0': [1, 2, 3, 4],
                '$metric0_dod': [1, 5, 9, 12],
                '$cumsum(metric0)': [1, 3, 6, 10],
                '$cumsum(metric0)_dod': [1, 6, 15, 27],
            }
        ).set_index('$timestamp')

    def test_formatting_heatmap_rule(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingHeatMapRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    'ff0000',
                )
            ],
        ).transform(self.df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']}],
                'data': [
                    {'$metric0': {'display': '1', 'raw': 1, 'color': 'fff2f2', 'text_color': '212121'}},
                    {'$metric0': {'display': '2', 'raw': 2, 'color': 'ffa2a2', 'text_color': '212121'}},
                    {'$metric0': {'display': '3', 'raw': 3, 'color': 'ff5151', 'text_color': '212121'}},
                    {'$metric0': {'display': '4', 'raw': 4, 'color': 'ff0000', 'text_color': 'FDFDFD'}},
                ],
            },
            result,
        )

    def test_formatting_heatmap_rule_with_reversed_color(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingHeatMapRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    'ff0000',
                    reverse_heatmap=True,
                )
            ],
        ).transform(self.df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']}],
                'data': [
                    {'$metric0': {'display': '1', 'raw': 1, 'color': 'ff0000', 'text_color': 'FDFDFD'}},
                    {'$metric0': {'display': '2', 'raw': 2, 'color': 'ff5151', 'text_color': '212121'}},
                    {'$metric0': {'display': '3', 'raw': 3, 'color': 'ffa2a2', 'text_color': '212121'}},
                    {'$metric0': {'display': '4', 'raw': 4, 'color': 'fff2f2', 'text_color': '212121'}},
                ],
            },
            result,
        )

    def test_formatting_heatmap_rule_with_just_one_value(self):
        # This test is to eliminate division by zero
        df = pd.DataFrame.from_dict(
            {
                '$metric0': [1],
                '$timestamp': [0],
            }
        ).set_index('$timestamp')

        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingHeatMapRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    'ff0000',
                )
            ],
        ).transform(df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']}],
                'data': [
                    {'$metric0': {'display': '1', 'raw': 1, 'color': 'FFFFFF', 'text_color': '212121'}},
                ],
            },
            result,
        )

    def test_formatting_heatmap_rule_with_2_colors(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingHeatMapRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    'ff0000',
                    start_color='00ff00',
                )
            ],
        ).transform(self.df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']}],
                'data': [
                    {'$metric0': {'display': '1', 'raw': 1, 'color': '00ff00', 'text_color': '212121'}},
                    {'$metric0': {'display': '2', 'raw': 2, 'color': 'aaffaa', 'text_color': '212121'}},
                    {'$metric0': {'display': '3', 'raw': 3, 'color': 'ffa2a2', 'text_color': '212121'}},
                    {'$metric0': {'display': '4', 'raw': 4, 'color': 'ff0000', 'text_color': 'FDFDFD'}},
                ],
            },
            result,
        )

    def test_single_formatting_condition_rule(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    ComparisonOperator.gt,
                    2,
                    'EEEEEE',
                )
            ],
        ).transform(self.df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']}],
                'data': [
                    {'$metric0': {'display': '1', 'raw': 1}},
                    {'$metric0': {'display': '2', 'raw': 2}},
                    {'$metric0': {'display': '3', 'raw': 3, 'color': 'EEEEEE', 'text_color': '212121'}},
                    {'$metric0': {'display': '4', 'raw': 4, 'color': 'EEEEEE', 'text_color': '212121'}},
                ],
            },
            result,
        )

    def test_rule_covers_row(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    ComparisonOperator.gt,
                    2,
                    'EEEEEE',
                    covers_row=True,
                )
            ],
        ).transform(self.df, [self.dataset.fields.timestamp], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']},
                ],
                'data': [
                    {
                        '$metric0': {'display': '1', 'raw': 1},
                        '$timestamp': {'display': '100', 'raw': 100},
                    },
                    {
                        '$metric0': {'display': '2', 'raw': 2},
                        '$timestamp': {'display': '200', 'raw': 200},
                    },
                    {
                        '$metric0': {'display': '3', 'raw': 3, 'color': 'EEEEEE', 'text_color': '212121'},
                        '$timestamp': {'display': '300', 'raw': 300, 'color': 'EEEEEE', 'text_color': '212121'},
                    },
                    {
                        '$metric0': {'display': '4', 'raw': 4, 'color': 'EEEEEE', 'text_color': '212121'},
                        '$timestamp': {'display': '400', 'raw': 400, 'color': 'EEEEEE', 'text_color': '212121'},
                    },
                ],
            },
            result,
        )

    def test_rule_covers_row_does_not_apply_when_pivoted(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    ComparisonOperator.gt,
                    2,
                    'EEEEEE',
                    covers_row=True,
                )
            ],
            pivot=[self.dataset.fields.timestamp],
        ).transform(self.df, [self.dataset.fields.timestamp], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': '100', 'accessor': '100', 'path_accessor': ['100']},
                    {'Header': '200', 'accessor': '200', 'path_accessor': ['200']},
                    {'Header': '300', 'accessor': '300', 'path_accessor': ['300']},
                    {'Header': '400', 'accessor': '400', 'path_accessor': ['400']},
                ],
                'data': [
                    {
                        '$metrics': {'raw': 'Metric0'},
                        '100': {'display': '1', 'raw': 1},
                        '200': {'display': '2', 'raw': 2},
                        '300': {'color': 'EEEEEE', 'display': '3', 'raw': 3, 'text_color': '212121'},
                        '400': {'color': 'EEEEEE', 'display': '4', 'raw': 4, 'text_color': '212121'},
                    }
                ],
            },
            result,
        )

    def test_formatting_a_reference(self):
        reference = DayOverDay(self.dataset.fields.timestamp)
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(
                        metric=self.dataset.fields.metric0,
                        reference=reference,
                    ),
                    ComparisonOperator.gt,
                    6,
                    'EEEEEE',
                )
            ],
        ).transform(self.df, [], [reference])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']},
                    {'Header': 'Metric0 DoD', 'accessor': '$metric0_dod', 'path_accessor': ['$metric0_dod']},
                ],
                'data': [
                    {
                        '$metric0': {'display': '1', 'raw': 1},
                        '$metric0_dod': {'display': '1', 'raw': 1},
                    },
                    {
                        '$metric0': {'display': '2', 'raw': 2},
                        '$metric0_dod': {'display': '5', 'raw': 5},
                    },
                    {
                        '$metric0': {'display': '3', 'raw': 3},
                        '$metric0_dod': {'display': '9', 'raw': 9, 'color': 'EEEEEE', 'text_color': '212121'},
                    },
                    {
                        '$metric0': {'display': '4', 'raw': 4},
                        '$metric0_dod': {'display': '12', 'raw': 12, 'color': 'EEEEEE', 'text_color': '212121'},
                    },
                ],
            },
            result,
        )

    def test_formatting_an_operation(self):
        operation = CumSum(self.dataset.fields.metric0)
        result = ReactTable(
            operation,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(operation=operation),
                    ComparisonOperator.gt,
                    5,
                    'EEEEEE',
                )
            ],
        ).transform(self.df, [], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'CumSum(Metric0)', 'accessor': '$cumsum(metric0)', 'path_accessor': ['$cumsum(metric0)']}
                ],
                'data': [
                    {'$cumsum(metric0)': {'display': '1', 'raw': 1}},
                    {'$cumsum(metric0)': {'display': '3', 'raw': 3}},
                    {'$cumsum(metric0)': {'display': '6', 'raw': 6, 'color': 'EEEEEE', 'text_color': '212121'}},
                    {'$cumsum(metric0)': {'display': '10', 'raw': 10, 'color': 'EEEEEE', 'text_color': '212121'}},
                ],
            },
            result,
        )

    def test_formatting_a_reference_with_an_operation(self):
        reference = DayOverDay(self.dataset.fields.timestamp)
        operation = CumSum(self.dataset.fields.metric0)
        result = ReactTable(
            operation,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(operation=operation, reference=reference),
                    ComparisonOperator.gt,
                    10,
                    'EEEEEE',
                )
            ],
        ).transform(self.df, [], [reference])

        self.assertEqual(
            {
                'columns': [
                    {
                        'Header': 'CumSum(Metric0)',
                        'accessor': '$cumsum(metric0)',
                        'path_accessor': ['$cumsum(metric0)'],
                    },
                    {
                        'Header': 'CumSum(Metric0) DoD',
                        'accessor': '$cumsum(metric0)_dod',
                        'path_accessor': ['$cumsum(metric0)_dod'],
                    },
                ],
                'data': [
                    {
                        '$cumsum(metric0)': {'display': '1', 'raw': 1},
                        '$cumsum(metric0)_dod': {'display': '1', 'raw': 1},
                    },
                    {
                        '$cumsum(metric0)': {'display': '3', 'raw': 3},
                        '$cumsum(metric0)_dod': {'display': '6', 'raw': 6},
                    },
                    {
                        '$cumsum(metric0)': {'display': '6', 'raw': 6},
                        '$cumsum(metric0)_dod': {'display': '15', 'raw': 15, 'color': 'EEEEEE', 'text_color': '212121'},
                    },
                    {
                        '$cumsum(metric0)': {'display': '10', 'raw': 10},
                        '$cumsum(metric0)_dod': {'display': '27', 'raw': 27, 'color': 'EEEEEE', 'text_color': '212121'},
                    },
                ],
            },
            result,
        )

    def test_multiple_formatting_condition_rule(self):
        result = ReactTable(
            self.dataset.fields.metric0,
            formatting_rules=[
                FormattingConditionRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    ComparisonOperator.gt,
                    3,
                    'EEEEEE',
                ),
                FormattingConditionRule(
                    FormattingField(metric=self.dataset.fields.metric0),
                    ComparisonOperator.lt,
                    2,
                    'AAAAAA',
                ),
            ],
        ).transform(self.df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Metric0', 'accessor': '$metric0', 'path_accessor': ['$metric0']}],
                'data': [
                    {'$metric0': {'display': '1', 'raw': 1, 'color': 'AAAAAA', 'text_color': '212121'}},
                    {'$metric0': {'display': '2', 'raw': 2}},
                    {'$metric0': {'display': '3', 'raw': 3}},
                    {'$metric0': {'display': '4', 'raw': 4, 'color': 'EEEEEE', 'text_color': '212121'}},
                ],
            },
            result,
        )


class ReactTableTransformerTests(TestCase):
    maxDiff = None

    def test_single_metric(self):
        result = ReactTable(mock_dataset.fields.votes).transform(dimx0_metricx1_df, [], [])

        self.assertEqual(
            {
                'columns': [{'Header': 'Votes', 'accessor': '$votes', 'path_accessor': ['$votes']}],
                'data': [{'$votes': {'display': '111,674,336', 'raw': 111674336}}],
            },
            result,
        )

    def test_multiple_metrics(self):
        result = ReactTable(mock_dataset.fields.votes, mock_dataset.fields.wins).transform(dimx0_metricx2_df, [], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Votes', 'accessor': '$votes', 'path_accessor': ['$votes']},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$votes': {'display': '111,674,336', 'raw': 111674336},
                        '$wins': {'display': '12', 'raw': 12},
                    }
                ],
            },
            result,
        )

    def test_multiple_metrics_reversed(self):
        result = ReactTable(mock_dataset.fields.wins, mock_dataset.fields.votes).transform(dimx0_metricx2_df, [], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                    {'Header': 'Votes', 'accessor': '$votes', 'path_accessor': ['$votes']},
                ],
                'data': [
                    {
                        '$votes': {'display': '111,674,336', 'raw': 111674336},
                        '$wins': {'display': '12', 'raw': 12},
                    }
                ],
            },
            result,
        )

    def test_time_series_dim(self):
        result = ReactTable(mock_dataset.fields.wins).transform(dimx1_date_df, [day(mock_dataset.fields.timestamp)], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {
                            'display': '2004-01-01',
                            'raw': '2004-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {
                            'display': '2008-01-01',
                            'raw': '2008-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {
                            'display': '2012-01-01',
                            'raw': '2012-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {
                            'display': '2016-01-01',
                            'raw': '2016-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2},
                    },
                ],
            },
            result,
        )

    def test_time_series_dim_with_operation(self):
        result = ReactTable(CumSum(mock_dataset.fields.votes)).transform(
            dimx1_date_operation_df, [day(mock_dataset.fields.timestamp)], []
        )

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'CumSum(Votes)', 'accessor': '$cumsum(votes)', 'path_accessor': ['$cumsum(votes)']},
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$cumsum(votes)': {'display': '15,220,449', 'raw': 15220449},
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$cumsum(votes)': {'display': '31,882,466', 'raw': 31882466},
                    },
                    {
                        '$timestamp': {
                            'display': '2004-01-01',
                            'raw': '2004-01-01T00:00:00',
                        },
                        '$cumsum(votes)': {'display': '51,497,398', 'raw': 51497398},
                    },
                    {
                        '$timestamp': {
                            'display': '2008-01-01',
                            'raw': '2008-01-01T00:00:00',
                        },
                        '$cumsum(votes)': {'display': '72,791,613', 'raw': 72791613},
                    },
                    {
                        '$timestamp': {
                            'display': '2012-01-01',
                            'raw': '2012-01-01T00:00:00',
                        },
                        '$cumsum(votes)': {'display': '93,363,823', 'raw': 93363823},
                    },
                    {
                        '$timestamp': {
                            'display': '2016-01-01',
                            'raw': '2016-01-01T00:00:00',
                        },
                        '$cumsum(votes)': {'display': '111,674,336', 'raw': 111674336},
                    },
                ],
            },
            result,
        )

    def test_dimx1_str(self):
        result = ReactTable(mock_dataset.fields.wins).transform(dimx1_str_df, [mock_dataset.fields.political_party], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$wins': {'display': '6', 'raw': 6.0},
                    },
                    {
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$political_party': {
                            'raw': 'Republican',
                            'hyperlink': 'http://example.com/Republican',
                        },
                        '$wins': {'display': '6', 'raw': 6.0},
                    },
                ],
            },
            result,
        )

    def test_dimx1_int(self):
        result = ReactTable(mock_dataset.fields.wins).transform(dimx1_num_df, [mock_dataset.fields['candidate-id']], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Candidate ID', 'accessor': '$candidate-id'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$candidate-id': {'display': '1', 'raw': 1.0},
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$candidate-id': {'display': '2', 'raw': 2.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '3', 'raw': 3.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '4', 'raw': 4.0},
                        '$wins': {'display': '4', 'raw': 4.0},
                    },
                    {
                        '$candidate-id': {'display': '5', 'raw': 5.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '6', 'raw': 6.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '7', 'raw': 7.0},
                        '$wins': {'display': '4', 'raw': 4.0},
                    },
                    {
                        '$candidate-id': {'display': '8', 'raw': 8.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '9', 'raw': 9.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '10', 'raw': 10.0},
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$candidate-id': {'display': '11', 'raw': 11.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_date_str(self):
        result = ReactTable(mock_dataset.fields.wins).transform(
            dimx2_date_str_df,
            [day(mock_dataset.fields.timestamp), mock_dataset.fields.political_party],
            [],
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_date_str_totals_date(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            Rollup(mock_dataset.fields.political_party),
        ]
        result = ReactTable(mock_dataset.fields.wins).transform(dimx2_date_str_totals_df, dimensions, [])

        self.assertIn('data', result)
        result['data'] = result['data'][-3:]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$timestamp': {
                            'display': '2016-01-01',
                            'raw': '2016-01-01T00:00:00',
                        },
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$political_party': {
                            'raw': 'Republican',
                            'hyperlink': 'http://example.com/Republican',
                        },
                        '$timestamp': {
                            'display': '2016-01-01',
                            'raw': '2016-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$political_party': {
                            'display': 'Totals',
                            'raw': '$totals',
                        },
                        '$timestamp': {
                            'display': '2016-01-01',
                            'raw': '2016-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_date_str_totals_all(self):
        dimensions = [
            Rollup(day(mock_dataset.fields.timestamp)),
            Rollup(mock_dataset.fields.political_party),
        ]
        result = ReactTable(mock_dataset.fields.wins).transform(dimx2_date_str_totalsx2_df, dimensions, [])
        self.assertIn('data', result)
        result['data'] = result['data'][:3] + result['data'][-1:]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$political_party': {
                            'raw': 'Republican',
                            'hyperlink': 'http://example.com/Republican',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$political_party': {
                            'display': 'Totals',
                            'raw': '$totals',
                        },
                        '$timestamp': {'display': 'Totals', 'raw': '$totals'},
                        '$wins': {'display': '12', 'raw': 12.0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_date_str_reference(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = ReactTable(mock_dataset.fields.votes).transform(dimx2_date_str_ref_df, dimensions, references)

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Votes', 'accessor': '$votes', 'path_accessor': ['$votes']},
                    {'Header': 'Votes EoE', 'accessor': '$votes_eoe', 'path_accessor': ['$votes_eoe']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Republican',
                            'hyperlink': 'http://example.com/Republican',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {'display': '6,564,547', 'raw': 6564547},
                        '$votes_eoe': {'display': '7,579,518', 'raw': 7579518},
                    },
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {'display': '8,294,949', 'raw': 8294949},
                        '$votes_eoe': {'display': '1,076,384', 'raw': 1076384},
                    },
                ],
            },
            result,
        )

    def test_dimx1_date_metricsx2_references(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = ReactTable(mock_dataset.fields.votes, mock_dataset.fields.wins).transform(
            dimx2_date_str_ref_df, dimensions, references
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Votes', 'accessor': '$votes', 'path_accessor': ['$votes']},
                    {'Header': 'Votes EoE', 'accessor': '$votes_eoe', 'path_accessor': ['$votes_eoe']},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                    {'Header': 'Wins EoE', 'accessor': '$wins_eoe', 'path_accessor': ['$wins_eoe']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Republican',
                            'hyperlink': 'http://example.com/Republican',
                        },
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {'display': '6,564,547', 'raw': 6564547},
                        '$votes_eoe': {'display': '7,579,518', 'raw': 7579518},
                        '$wins': {'display': '0', 'raw': 0},
                        '$wins_eoe': {'display': '2', 'raw': 2},
                    },
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {'display': '8,294,949', 'raw': 8294949},
                        '$votes_eoe': {'display': '1,076,384', 'raw': 1076384},
                        '$wins': {'display': '0', 'raw': 0},
                        '$wins_eoe': {'display': '0', 'raw': 0},
                    },
                ],
            },
            result,
        )

    def test_transpose(self):
        dimensions = [mock_dataset.fields.political_party]
        result = ReactTable(mock_dataset.fields.wins, transpose=True).transform(dimx1_str_df, dimensions, [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': 'Democrat', 'accessor': 'Democrat', 'path_accessor': ['Democrat']},
                    {'Header': 'Independent', 'accessor': 'Independent', 'path_accessor': ['Independent']},
                    {'Header': 'Republican', 'accessor': 'Republican', 'path_accessor': ['Republican']},
                ],
                'data': [
                    {
                        '$metrics': {'raw': 'Wins'},
                        'Democrat': {'display': '6', 'raw': 6.0},
                        'Independent': {'display': '0', 'raw': 0.0},
                        'Republican': {'display': '6', 'raw': 6.0},
                    }
                ],
            },
            result,
        )

    def test_transpose_without_dimension(self):
        result = ReactTable(mock_dataset.fields.votes, mock_dataset.fields.wins, transpose=True).transform(
            dimx1_none_df, [], []
        )

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': '', 'accessor': '0', 'path_accessor': ['0']},
                ],
                'data': [
                    {
                        0: {'display': '111,674,336', 'raw': 111674336},
                        '$metrics': {'raw': 'Votes'},
                    },
                    {0: {'display': '12', 'raw': 12}, '$metrics': {'raw': 'Wins'}},
                ],
            },
            result,
        )

    def test_dimx2_hide_dim1(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        result = ReactTable(mock_dataset.fields.wins, hide=[mock_dataset.fields.political_party]).transform(
            dimx2_date_str_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$timestamp': {'display': '1996-01-01', 'raw': '1996-01-01T00:00:00'},
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {'display': '1996-01-01', 'raw': '1996-01-01T00:00:00'},
                        '$wins': {'display': '0', 'raw': 0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_metricx2_refx2_hide_metrics(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = ReactTable(
            mock_dataset.fields.votes,
            mock_dataset.fields.wins,
            hide=[mock_dataset.fields.votes, mock_dataset.fields.wins],
        ).transform(dimx2_date_str_ref_df, dimensions, references)

        self.assertIn("data", result)
        result["data"] = result["data"][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                "columns": [
                    {"Header": "Timestamp", "accessor": "$timestamp"},
                    {"Header": "Party", "accessor": "$political_party"},
                    {"Header": "Votes EoE", "accessor": "$votes_eoe", 'path_accessor': ['$votes_eoe']},
                    {"Header": "Wins EoE", "accessor": "$wins_eoe", 'path_accessor': ['$wins_eoe']},
                ],
                "data": [
                    {
                        "$political_party": {
                            "raw": "Republican",
                            "hyperlink": "http://example.com/Republican",
                        },
                        "$timestamp": {
                            "display": "1996-01-01",
                            "raw": "1996-01-01T00:00:00",
                        },
                        "$votes_eoe": {"display": "7,579,518", "raw": 7579518.0},
                        "$wins_eoe": {"display": "2", "raw": 2.0},
                    },
                    {
                        "$political_party": {
                            "raw": "Democrat",
                            "hyperlink": "http://example.com/Democrat",
                        },
                        "$timestamp": {
                            "display": "2000-01-01",
                            "raw": "2000-01-01T00:00:00",
                        },
                        "$votes_eoe": {"display": "1,076,384", "raw": 1076384.0},
                        "$wins_eoe": {"display": "0", "raw": 0.0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_fetch_only_dim1(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        dimensions[1].fetch_only = True
        result = ReactTable(mock_dataset.fields.wins).transform(dimx2_date_str_df, dimensions, [])
        dimensions[1].fetch_only = False

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$timestamp': {'display': '1996-01-01', 'raw': '1996-01-01T00:00:00'},
                        '$wins': {'display': '2', 'raw': 2},
                    },
                    {
                        '$timestamp': {'display': '1996-01-01', 'raw': '1996-01-01T00:00:00'},
                        '$wins': {'display': '0', 'raw': 0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_pivot_dim1(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        result = ReactTable(mock_dataset.fields.wins, pivot=[mock_dataset.fields.timestamp]).transform(
            dimx2_date_str_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {
                        'Header': '1996-01-01',
                        'accessor': '$wins.1996-01-01T00:00:00',
                        'path_accessor': ['$wins', '1996-01-01T00:00:00'],
                    },
                    {
                        'Header': '2000-01-01',
                        'accessor': '$wins.2000-01-01T00:00:00',
                        'path_accessor': ['$wins', '2000-01-01T00:00:00'],
                    },
                    {
                        'Header': '2004-01-01',
                        'accessor': '$wins.2004-01-01T00:00:00',
                        'path_accessor': ['$wins', '2004-01-01T00:00:00'],
                    },
                    {
                        'Header': '2008-01-01',
                        'accessor': '$wins.2008-01-01T00:00:00',
                        'path_accessor': ['$wins', '2008-01-01T00:00:00'],
                    },
                    {
                        'Header': '2012-01-01',
                        'accessor': '$wins.2012-01-01T00:00:00',
                        'path_accessor': ['$wins', '2012-01-01T00:00:00'],
                    },
                    {
                        'Header': '2016-01-01',
                        'accessor': '$wins.2016-01-01T00:00:00',
                        'path_accessor': ['$wins', '2016-01-01T00:00:00'],
                    },
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$wins': {
                            '1996-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                            '2000-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                            '2004-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                            '2008-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                            '2012-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                            '2016-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        },
                    },
                    {
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$wins': {
                            '1996-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                            '2000-01-01T00:00:00': {'display': '', 'raw': None},
                            '2004-01-01T00:00:00': {'display': '', 'raw': None},
                            '2008-01-01T00:00:00': {'display': '', 'raw': None},
                            '2012-01-01T00:00:00': {'display': '', 'raw': None},
                            '2016-01-01T00:00:00': {'display': '', 'raw': None},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx2_pivot_dim1_with_sorting(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        result = ReactTable(mock_dataset.fields.wins, pivot=[mock_dataset.fields.timestamp], sort=[0]).transform(
            dimx2_date_str_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {
                        'Header': '1996-01-01',
                        'accessor': '$wins.1996-01-01T00:00:00',
                        'path_accessor': ['$wins', '1996-01-01T00:00:00'],
                    },
                    {
                        'Header': '2000-01-01',
                        'accessor': '$wins.2000-01-01T00:00:00',
                        'path_accessor': ['$wins', '2000-01-01T00:00:00'],
                    },
                    {
                        'Header': '2004-01-01',
                        'accessor': '$wins.2004-01-01T00:00:00',
                        'path_accessor': ['$wins', '2004-01-01T00:00:00'],
                    },
                    {
                        'Header': '2008-01-01',
                        'accessor': '$wins.2008-01-01T00:00:00',
                        'path_accessor': ['$wins', '2008-01-01T00:00:00'],
                    },
                    {
                        'Header': '2012-01-01',
                        'accessor': '$wins.2012-01-01T00:00:00',
                        'path_accessor': ['$wins', '2012-01-01T00:00:00'],
                    },
                    {
                        'Header': '2016-01-01',
                        'accessor': '$wins.2016-01-01T00:00:00',
                        'path_accessor': ['$wins', '2016-01-01T00:00:00'],
                    },
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$wins': {
                            '1996-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                            '2000-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                            '2004-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                            '2008-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                            '2012-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                            '2016-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        },
                    },
                    {
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$wins': {
                            '1996-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                            '2000-01-01T00:00:00': {'display': '', 'raw': None},
                            '2004-01-01T00:00:00': {'display': '', 'raw': None},
                            '2008-01-01T00:00:00': {'display': '', 'raw': None},
                            '2012-01-01T00:00:00': {'display': '', 'raw': None},
                            '2016-01-01T00:00:00': {'display': '', 'raw': None},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx2_pivot_dim2(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        result = ReactTable(mock_dataset.fields.wins, pivot=[mock_dataset.fields.political_party]).transform(
            dimx2_date_str_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Democrat', 'accessor': '$wins.Democrat', 'path_accessor': ['$wins', 'Democrat']},
                    {
                        'Header': 'Independent',
                        'accessor': '$wins.Independent',
                        'path_accessor': ['$wins', 'Independent'],
                    },
                    {'Header': 'Republican', 'accessor': '$wins.Republican', 'path_accessor': ['$wins', 'Republican']},
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {
                            'Democrat': {'display': '2', 'raw': 2.0},
                            'Independent': {'display': '0', 'raw': 0.0},
                            'Republican': {'display': '0', 'raw': 0.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$wins': {
                            'Democrat': {'display': '0', 'raw': 0.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '2', 'raw': 2.0},
                        },
                    },
                ],
            },
            result,
        )

    def test_metricx2_pivot_dim2(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        result = ReactTable(
            mock_dataset.fields.wins,
            mock_dataset.fields.votes,
            pivot=[mock_dataset.fields.political_party],
        ).transform(dimx2_date_str_df, dimensions, [])

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {
                        'Header': 'Votes',
                        'columns': [
                            {
                                'Header': 'Democrat',
                                'accessor': '$votes.Democrat',
                                'path_accessor': ['$votes', 'Democrat'],
                            },
                            {
                                'Header': 'Independent',
                                'accessor': '$votes.Independent',
                                'path_accessor': ['$votes', 'Independent'],
                            },
                            {
                                'Header': 'Republican',
                                'accessor': '$votes.Republican',
                                'path_accessor': ['$votes', 'Republican'],
                            },
                        ],
                    },
                    {
                        'Header': 'Wins',
                        'columns': [
                            {
                                'Header': 'Democrat',
                                'accessor': '$wins.Democrat',
                                'path_accessor': ['$wins', 'Democrat'],
                            },
                            {
                                'Header': 'Independent',
                                'accessor': '$wins.Independent',
                                'path_accessor': ['$wins', 'Independent'],
                            },
                            {
                                'Header': 'Republican',
                                'accessor': '$wins.Republican',
                                'path_accessor': ['$wins', 'Republican'],
                            },
                        ],
                    },
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {
                            'Democrat': {'display': '7,579,518', 'raw': 7579518},
                            'Independent': {'display': '1,076,384', 'raw': 1076384},
                            'Republican': {'display': '6,564,547', 'raw': 6564547},
                        },
                        '$wins': {
                            'Democrat': {'display': '2', 'raw': 2},
                            'Independent': {'display': '0', 'raw': 0},
                            'Republican': {'display': '0', 'raw': 0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {
                            'Democrat': {'display': '8,294,949', 'raw': 8294949},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '8,367,068', 'raw': 8367068},
                        },
                        '$wins': {
                            'Democrat': {'display': '0', 'raw': 0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '2', 'raw': 2},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx2_metricx2_refx2_pivot_dim2(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = ReactTable(
            mock_dataset.fields.votes,
            mock_dataset.fields.wins,
            pivot=[mock_dataset.fields.political_party],
        ).transform(dimx2_date_str_ref_df, dimensions, references)

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {
                        'Header': 'Votes',
                        'columns': [
                            {
                                'Header': 'Republican',
                                'accessor': '$votes.Republican',
                                'path_accessor': ['$votes', 'Republican'],
                            },
                            {
                                'Header': 'Democrat',
                                'accessor': '$votes.Democrat',
                                'path_accessor': ['$votes', 'Democrat'],
                            },
                        ],
                    },
                    {
                        'Header': 'Votes EoE',
                        'columns': [
                            {
                                'Header': 'Republican',
                                'accessor': '$votes_eoe.Republican',
                                'path_accessor': ['$votes_eoe', 'Republican'],
                            },
                            {
                                'Header': 'Democrat',
                                'accessor': '$votes_eoe.Democrat',
                                'path_accessor': ['$votes_eoe', 'Democrat'],
                            },
                        ],
                    },
                    {
                        'Header': 'Wins',
                        'columns': [
                            {
                                'Header': 'Republican',
                                'accessor': '$wins.Republican',
                                'path_accessor': ['$wins', 'Republican'],
                            },
                            {
                                'Header': 'Democrat',
                                'accessor': '$wins.Democrat',
                                'path_accessor': ['$wins', 'Democrat'],
                            },
                        ],
                    },
                    {
                        'Header': 'Wins EoE',
                        'columns': [
                            {
                                'Header': 'Republican',
                                'accessor': '$wins_eoe.Republican',
                                'path_accessor': ['$wins_eoe', 'Republican'],
                            },
                            {
                                'Header': 'Democrat',
                                'accessor': '$wins_eoe.Democrat',
                                'path_accessor': ['$wins_eoe', 'Democrat'],
                            },
                        ],
                    },
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {
                            'Democrat': {'display': '', 'raw': None},
                            'Republican': {'display': '6,564,547', 'raw': 6564547.0},
                        },
                        '$votes_eoe': {
                            'Democrat': {'display': '', 'raw': None},
                            'Republican': {'display': '7,579,518', 'raw': 7579518.0},
                        },
                        '$wins': {
                            'Democrat': {'display': '', 'raw': None},
                            'Republican': {'display': '0', 'raw': 0.0},
                        },
                        '$wins_eoe': {
                            'Democrat': {'display': '', 'raw': None},
                            'Republican': {'display': '2', 'raw': 2.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {
                            'Democrat': {'display': '8,294,949', 'raw': 8294949.0},
                            'Republican': {'display': '8,367,068', 'raw': 8367068.0},
                        },
                        '$votes_eoe': {
                            'Democrat': {'display': '1,076,384', 'raw': 1076384.0},
                            'Republican': {'display': '6,564,547', 'raw': 6564547.0},
                        },
                        '$wins': {
                            'Democrat': {'display': '0', 'raw': 0.0},
                            'Republican': {'display': '2', 'raw': 2.0},
                        },
                        '$wins_eoe': {
                            'Democrat': {'display': '0', 'raw': 0.0},
                            'Republican': {'display': '0', 'raw': 0.0},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx1_int_metricx1_pivot_dim1_same_as_transpose(self):
        result = ReactTable(mock_dataset.fields.wins, pivot=[mock_dataset.fields['candidate-id']]).transform(
            dimx1_num_df, [mock_dataset.fields['candidate-id']], []
        )

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': '1', 'accessor': '1', 'path_accessor': ['1']},
                    {'Header': '2', 'accessor': '2', 'path_accessor': ['2']},
                    {'Header': '3', 'accessor': '3', 'path_accessor': ['3']},
                    {'Header': '4', 'accessor': '4', 'path_accessor': ['4']},
                    {'Header': '5', 'accessor': '5', 'path_accessor': ['5']},
                    {'Header': '6', 'accessor': '6', 'path_accessor': ['6']},
                    {'Header': '7', 'accessor': '7', 'path_accessor': ['7']},
                    {'Header': '8', 'accessor': '8', 'path_accessor': ['8']},
                    {'Header': '9', 'accessor': '9', 'path_accessor': ['9']},
                    {'Header': '10', 'accessor': '10', 'path_accessor': ['10']},
                    {'Header': '11', 'accessor': '11', 'path_accessor': ['11']},
                ],
                'data': [
                    {
                        '$metrics': {'raw': 'Wins'},
                        '1': {'display': '2', 'raw': 2.0},
                        '2': {'display': '0', 'raw': 0.0},
                        '3': {'display': '0', 'raw': 0.0},
                        '4': {'display': '4', 'raw': 4.0},
                        '5': {'display': '0', 'raw': 0.0},
                        '6': {'display': '0', 'raw': 0.0},
                        '7': {'display': '4', 'raw': 4.0},
                        '8': {'display': '0', 'raw': 0.0},
                        '9': {'display': '0', 'raw': 0.0},
                        '10': {'display': '2', 'raw': 2.0},
                        '11': {'display': '0', 'raw': 0.0},
                    }
                ],
            },
            result,
        )

    def test_dimx1_int_metricx1_transpose(self):
        result = ReactTable(
            mock_dataset.fields.wins,
            pivot=[mock_dataset.fields['candidate-id']],
            transpose=True,
        ).transform(dimx1_num_df, [mock_dataset.fields['candidate-id']], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Candidate ID', 'accessor': '$candidate-id'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$candidate-id': {'display': '1', 'raw': 1.0},
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$candidate-id': {'display': '2', 'raw': 2.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '3', 'raw': 3.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '4', 'raw': 4.0},
                        '$wins': {'display': '4', 'raw': 4.0},
                    },
                    {
                        '$candidate-id': {'display': '5', 'raw': 5.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '6', 'raw': 6.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '7', 'raw': 7.0},
                        '$wins': {'display': '4', 'raw': 4.0},
                    },
                    {
                        '$candidate-id': {'display': '8', 'raw': 8.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '9', 'raw': 9.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$candidate-id': {'display': '10', 'raw': 10.0},
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$candidate-id': {'display': '11', 'raw': 11.0},
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                ],
            },
            result,
        )

    def test_dimx1_int_metricx2_pivot(self):
        result = ReactTable(
            mock_dataset.fields.wins,
            mock_dataset.fields.votes,
            pivot=[mock_dataset.fields['candidate-id']],
        ).transform(dimx1_num_df, [mock_dataset.fields['candidate-id']], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': '1', 'accessor': '1', 'path_accessor': ['1']},
                    {'Header': '2', 'accessor': '2', 'path_accessor': ['2']},
                    {'Header': '3', 'accessor': '3', 'path_accessor': ['3']},
                    {'Header': '4', 'accessor': '4', 'path_accessor': ['4']},
                    {'Header': '5', 'accessor': '5', 'path_accessor': ['5']},
                    {'Header': '6', 'accessor': '6', 'path_accessor': ['6']},
                    {'Header': '7', 'accessor': '7', 'path_accessor': ['7']},
                    {'Header': '8', 'accessor': '8', 'path_accessor': ['8']},
                    {'Header': '9', 'accessor': '9', 'path_accessor': ['9']},
                    {'Header': '10', 'accessor': '10', 'path_accessor': ['10']},
                    {'Header': '11', 'accessor': '11', 'path_accessor': ['11']},
                ],
                'data': [
                    {
                        '1': {'display': '2', 'raw': 2.0},
                        '2': {'display': '0', 'raw': 0.0},
                        '3': {'display': '0', 'raw': 0.0},
                        '4': {'display': '4', 'raw': 4.0},
                        '5': {'display': '0', 'raw': 0.0},
                        '6': {'display': '0', 'raw': 0.0},
                        '7': {'display': '4', 'raw': 4.0},
                        '8': {'display': '0', 'raw': 0.0},
                        '9': {'display': '0', 'raw': 0.0},
                        '10': {'display': '2', 'raw': 2.0},
                        '11': {'display': '0', 'raw': 0.0},
                        '$metrics': {'raw': 'Wins'},
                    },
                    {
                        '1': {'display': '7,579,518', 'raw': 7579518.0},
                        '2': {'display': '6,564,547', 'raw': 6564547.0},
                        '3': {'display': '1,076,384', 'raw': 1076384.0},
                        '4': {'display': '18,403,811', 'raw': 18403811.0},
                        '5': {'display': '8,294,949', 'raw': 8294949.0},
                        '6': {'display': '9,578,189', 'raw': 9578189.0},
                        '7': {'display': '24,227,234', 'raw': 24227234.0},
                        '8': {'display': '9,491,109', 'raw': 9491109.0},
                        '9': {'display': '8,148,082', 'raw': 8148082.0},
                        '10': {'display': '13,438,835', 'raw': 13438835.0},
                        '11': {'display': '4,871,678', 'raw': 4871678.0},
                        '$metrics': {'raw': 'Votes'},
                    },
                ],
            },
            result,
        )

    def test_dimx1_date_metricx1(self):
        result = ReactTable(mock_dataset.fields.wins).transform(dimx1_date_df, [day(mock_dataset.fields.timestamp)], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$timestamp': {
                            'display': '2004-01-01',
                            'raw': '2004-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$timestamp': {
                            'display': '2008-01-01',
                            'raw': '2008-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$timestamp': {
                            'display': '2012-01-01',
                            'raw': '2012-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                    {
                        '$timestamp': {
                            'display': '2016-01-01',
                            'raw': '2016-01-01T00:00:00',
                        },
                        '$wins': {'display': '2', 'raw': 2.0},
                    },
                ],
            },
            result,
        )

    def test_dimx2_metricx1_pivot_dim2_rollup_dim2(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            Rollup(mock_dataset.fields.political_party),
        ]
        result = ReactTable(mock_dataset.fields.votes, pivot=[mock_dataset.fields.political_party]).transform(
            dimx2_date_str_totalsx2_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2] + result['data'][-1:]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {'Header': 'Democrat', 'accessor': '$votes.Democrat', 'path_accessor': ['$votes', 'Democrat']},
                    {
                        'Header': 'Independent',
                        'accessor': '$votes.Independent',
                        'path_accessor': ['$votes', 'Independent'],
                    },
                    {
                        'Header': 'Republican',
                        'accessor': '$votes.Republican',
                        'path_accessor': ['$votes', 'Republican'],
                    },
                    {
                        'Header': 'Totals',
                        'accessor': '$votes.$totals',
                        'path_accessor': ['$votes', '$totals'],
                        'className': 'fireant-totals',
                    },
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '15,220,449', 'raw': 15220449.0},
                            'Democrat': {'display': '7,579,518', 'raw': 7579518.0},
                            'Independent': {'display': '1,076,384', 'raw': 1076384.0},
                            'Republican': {'display': '6,564,547', 'raw': 6564547.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '16,662,017', 'raw': 16662017.0},
                            'Democrat': {'display': '8,294,949', 'raw': 8294949.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '8,367,068', 'raw': 8367068.0},
                        },
                    },
                    {
                        '$timestamp': {'display': 'Totals', 'raw': '$totals'},
                        '$votes': {
                            '$totals': {'display': '111,674,336', 'raw': 111674336.0},
                            'Democrat': {'display': '', 'raw': None},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '', 'raw': None},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx2_date_str_pivot_dim2_rollup_all(self):
        political_party = Rollup(mock_dataset.fields.political_party)
        dimensions = [Rollup(day(mock_dataset.fields.timestamp)), political_party]
        result = ReactTable(mock_dataset.fields.wins, mock_dataset.fields.votes, pivot=[political_party]).transform(
            dimx2_date_str_totalsx2_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2] + result['data'][-1:]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {
                        'Header': 'Votes',
                        'columns': [
                            {
                                'Header': 'Democrat',
                                'accessor': '$votes.Democrat',
                                'path_accessor': ['$votes', 'Democrat'],
                            },
                            {
                                'Header': 'Independent',
                                'accessor': '$votes.Independent',
                                'path_accessor': ['$votes', 'Independent'],
                            },
                            {
                                'Header': 'Republican',
                                'accessor': '$votes.Republican',
                                'path_accessor': ['$votes', 'Republican'],
                            },
                            {
                                'Header': 'Totals',
                                'accessor': '$votes.$totals',
                                'path_accessor': ['$votes', '$totals'],
                                'className': 'fireant-totals',
                            },
                        ],
                    },
                    {
                        'Header': 'Wins',
                        'columns': [
                            {
                                'Header': 'Democrat',
                                'accessor': '$wins.Democrat',
                                'path_accessor': ['$wins', 'Democrat'],
                            },
                            {
                                'Header': 'Independent',
                                'accessor': '$wins.Independent',
                                'path_accessor': ['$wins', 'Independent'],
                            },
                            {
                                'Header': 'Republican',
                                'accessor': '$wins.Republican',
                                'path_accessor': ['$wins', 'Republican'],
                            },
                            {
                                'Header': 'Totals',
                                'accessor': '$wins.$totals',
                                'path_accessor': ['$wins', '$totals'],
                                'className': 'fireant-totals',
                            },
                        ],
                    },
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '15,220,449', 'raw': 15220449.0},
                            'Democrat': {'display': '7,579,518', 'raw': 7579518.0},
                            'Independent': {'display': '1,076,384', 'raw': 1076384.0},
                            'Republican': {'display': '6,564,547', 'raw': 6564547.0},
                        },
                        '$wins': {
                            '$totals': {'display': '2', 'raw': 2.0},
                            'Democrat': {'display': '2', 'raw': 2.0},
                            'Independent': {'display': '0', 'raw': 0.0},
                            'Republican': {'display': '0', 'raw': 0.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '16,662,017', 'raw': 16662017.0},
                            'Democrat': {'display': '8,294,949', 'raw': 8294949.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '8,367,068', 'raw': 8367068.0},
                        },
                        '$wins': {
                            '$totals': {'display': '2', 'raw': 2.0},
                            'Democrat': {'display': '0', 'raw': 0.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '2', 'raw': 2.0},
                        },
                    },
                    {
                        '$timestamp': {'display': 'Totals', 'raw': '$totals'},
                        '$votes': {
                            '$totals': {'display': '111,674,336', 'raw': 111674336.0},
                            'Democrat': {'display': '', 'raw': None},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '', 'raw': None},
                        },
                        '$wins': {
                            '$totals': {'display': '12', 'raw': 12.0},
                            'Democrat': {'display': '', 'raw': None},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '', 'raw': None},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx2_pivot_both_dims_and_transpose(self):
        political_party = Rollup(mock_dataset.fields.political_party)
        dimensions = [Rollup(day(mock_dataset.fields.timestamp)), political_party]
        result = ReactTable(mock_dataset.fields.wins, mock_dataset.fields.votes, pivot=[political_party]).transform(
            dimx2_date_str_totalsx2_df, dimensions, []
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:4]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Timestamp', 'accessor': '$timestamp'},
                    {
                        'Header': 'Votes',
                        'columns': [
                            {
                                'Header': 'Democrat',
                                'accessor': '$votes.Democrat',
                                'path_accessor': ['$votes', 'Democrat'],
                            },
                            {
                                'Header': 'Independent',
                                'accessor': '$votes.Independent',
                                'path_accessor': ['$votes', 'Independent'],
                            },
                            {
                                'Header': 'Republican',
                                'accessor': '$votes.Republican',
                                'path_accessor': ['$votes', 'Republican'],
                            },
                            {
                                'Header': 'Totals',
                                'accessor': '$votes.$totals',
                                'path_accessor': ['$votes', '$totals'],
                                'className': 'fireant-totals',
                            },
                        ],
                    },
                    {
                        'Header': 'Wins',
                        'columns': [
                            {
                                'Header': 'Democrat',
                                'accessor': '$wins.Democrat',
                                'path_accessor': ['$wins', 'Democrat'],
                            },
                            {
                                'Header': 'Independent',
                                'accessor': '$wins.Independent',
                                'path_accessor': ['$wins', 'Independent'],
                            },
                            {
                                'Header': 'Republican',
                                'accessor': '$wins.Republican',
                                'path_accessor': ['$wins', 'Republican'],
                            },
                            {
                                'Header': 'Totals',
                                'accessor': '$wins.$totals',
                                'path_accessor': ['$wins', '$totals'],
                                'className': 'fireant-totals',
                            },
                        ],
                    },
                ],
                'data': [
                    {
                        '$timestamp': {
                            'display': '1996-01-01',
                            'raw': '1996-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '15,220,449', 'raw': 15220449.0},
                            'Democrat': {'display': '7,579,518', 'raw': 7579518.0},
                            'Independent': {'display': '1,076,384', 'raw': 1076384.0},
                            'Republican': {'display': '6,564,547', 'raw': 6564547.0},
                        },
                        '$wins': {
                            '$totals': {'display': '2', 'raw': 2.0},
                            'Democrat': {'display': '2', 'raw': 2.0},
                            'Independent': {'display': '0', 'raw': 0.0},
                            'Republican': {'display': '0', 'raw': 0.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2000-01-01',
                            'raw': '2000-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '16,662,017', 'raw': 16662017.0},
                            'Democrat': {'display': '8,294,949', 'raw': 8294949.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '8,367,068', 'raw': 8367068.0},
                        },
                        '$wins': {
                            '$totals': {'display': '2', 'raw': 2.0},
                            'Democrat': {'display': '0', 'raw': 0.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '2', 'raw': 2.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2004-01-01',
                            'raw': '2004-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '19,614,932', 'raw': 19614932.0},
                            'Democrat': {'display': '9,578,189', 'raw': 9578189.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '10,036,743', 'raw': 10036743.0},
                        },
                        '$wins': {
                            '$totals': {'display': '2', 'raw': 2.0},
                            'Democrat': {'display': '0', 'raw': 0.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '2', 'raw': 2.0},
                        },
                    },
                    {
                        '$timestamp': {
                            'display': '2008-01-01',
                            'raw': '2008-01-01T00:00:00',
                        },
                        '$votes': {
                            '$totals': {'display': '21,294,215', 'raw': 21294215.0},
                            'Democrat': {'display': '11,803,106', 'raw': 11803106.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '9,491,109', 'raw': 9491109.0},
                        },
                        '$wins': {
                            '$totals': {'display': '2', 'raw': 2.0},
                            'Democrat': {'display': '2', 'raw': 2.0},
                            'Independent': {'display': '', 'raw': None},
                            'Republican': {'display': '0', 'raw': 0.0},
                        },
                    },
                ],
            },
            result,
        )

    def test_dimx2_date_str_pivot_dim2_transpose_rollup_all(self):
        political_party = Rollup(mock_dataset.fields.political_party)
        dimensions = [Rollup(day(mock_dataset.fields.timestamp)), political_party]
        result = ReactTable(
            mock_dataset.fields.wins,
            mock_dataset.fields.votes,
            pivot=[political_party],
            transpose=True,
        ).transform(dimx2_date_str_totalsx2_df, dimensions, [])

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {
                        'Header': '1996-01-01',
                        'accessor': '1996-01-01T00:00:00',
                        'path_accessor': ['1996-01-01T00:00:00'],
                    },
                    {
                        'Header': '2000-01-01',
                        'accessor': '2000-01-01T00:00:00',
                        'path_accessor': ['2000-01-01T00:00:00'],
                    },
                    {
                        'Header': '2004-01-01',
                        'accessor': '2004-01-01T00:00:00',
                        'path_accessor': ['2004-01-01T00:00:00'],
                    },
                    {
                        'Header': '2008-01-01',
                        'accessor': '2008-01-01T00:00:00',
                        'path_accessor': ['2008-01-01T00:00:00'],
                    },
                    {
                        'Header': '2012-01-01',
                        'accessor': '2012-01-01T00:00:00',
                        'path_accessor': ['2012-01-01T00:00:00'],
                    },
                    {
                        'Header': '2016-01-01',
                        'accessor': '2016-01-01T00:00:00',
                        'path_accessor': ['2016-01-01T00:00:00'],
                    },
                    {
                        'Header': 'Totals',
                        'accessor': '$totals',
                        'path_accessor': ['$totals'],
                        'className': 'fireant-totals',
                    },
                ],
                'data': [
                    {
                        '$metrics': {'raw': 'Wins'},
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$totals': {'display': '', 'raw': None},
                        '1996-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                        '2000-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        '2004-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        '2008-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                        '2012-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                        '2016-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$metrics': {'raw': 'Wins'},
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$totals': {'display': '', 'raw': None},
                        '1996-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        '2000-01-01T00:00:00': {'display': '', 'raw': None},
                        '2004-01-01T00:00:00': {'display': '', 'raw': None},
                        '2008-01-01T00:00:00': {'display': '', 'raw': None},
                        '2012-01-01T00:00:00': {'display': '', 'raw': None},
                        '2016-01-01T00:00:00': {'display': '', 'raw': None},
                    },
                ],
            },
            result,
        )

    def test_dimx2_pivot_dim2_rollup_all_no_rollup_on_pivot_arg(self):
        dimensions = [
            Rollup(day(mock_dataset.fields.timestamp)),
            Rollup(mock_dataset.fields.political_party),
        ]
        result = ReactTable(
            mock_dataset.fields.wins,
            mock_dataset.fields.votes,
            pivot=[mock_dataset.fields.political_party],
            transpose=True,
        ).transform(dimx2_date_str_totalsx2_df, dimensions, [])

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {
                        'Header': '1996-01-01',
                        'accessor': '1996-01-01T00:00:00',
                        'path_accessor': ['1996-01-01T00:00:00'],
                    },
                    {
                        'Header': '2000-01-01',
                        'accessor': '2000-01-01T00:00:00',
                        'path_accessor': ['2000-01-01T00:00:00'],
                    },
                    {
                        'Header': '2004-01-01',
                        'accessor': '2004-01-01T00:00:00',
                        'path_accessor': ['2004-01-01T00:00:00'],
                    },
                    {
                        'Header': '2008-01-01',
                        'accessor': '2008-01-01T00:00:00',
                        'path_accessor': ['2008-01-01T00:00:00'],
                    },
                    {
                        'Header': '2012-01-01',
                        'accessor': '2012-01-01T00:00:00',
                        'path_accessor': ['2012-01-01T00:00:00'],
                    },
                    {
                        'Header': '2016-01-01',
                        'accessor': '2016-01-01T00:00:00',
                        'path_accessor': ['2016-01-01T00:00:00'],
                    },
                    {
                        'Header': 'Totals',
                        'accessor': '$totals',
                        'path_accessor': ['$totals'],
                        'className': 'fireant-totals',
                    },
                ],
                'data': [
                    {
                        '$metrics': {'raw': 'Wins'},
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$totals': {'display': '', 'raw': None},
                        '1996-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                        '2000-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        '2004-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        '2008-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                        '2012-01-01T00:00:00': {'display': '2', 'raw': 2.0},
                        '2016-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$metrics': {'raw': 'Wins'},
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$totals': {'display': '', 'raw': None},
                        '1996-01-01T00:00:00': {'display': '0', 'raw': 0.0},
                        '2000-01-01T00:00:00': {'display': '', 'raw': None},
                        '2004-01-01T00:00:00': {'display': '', 'raw': None},
                        '2008-01-01T00:00:00': {'display': '', 'raw': None},
                        '2012-01-01T00:00:00': {'display': '', 'raw': None},
                        '2016-01-01T00:00:00': {'display': '', 'raw': None},
                    },
                ],
            },
            result,
        )


class ReactTableHyperlinkTransformerTests(TestCase):
    maxDiff = None

    def test_add_hyperlink_with_formatted_values(self):
        result = ReactTable(mock_dataset.fields.wins).transform(dimx1_str_df, [mock_dataset.fields.political_party], [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'hyperlink': 'http://example.com/Democrat',
                            'raw': 'Democrat',
                        },
                        '$wins': {'display': '6', 'raw': 6.0},
                    },
                    {
                        '$political_party': {
                            'hyperlink': 'http://example.com/Independent',
                            'raw': 'Independent',
                        },
                        '$wins': {'display': '0', 'raw': 0.0},
                    },
                    {
                        '$political_party': {
                            'hyperlink': 'http://example.com/Republican',
                            'raw': 'Republican',
                        },
                        '$wins': {'display': '6', 'raw': 6.0},
                    },
                ],
            },
            result,
        )

    def test_do_not_add_hyperlink_to_pivoted_dimensions(self):
        dimensions = [mock_dataset.fields.political_party]
        result = ReactTable(mock_dataset.fields.wins, pivot=dimensions).transform(dimx1_str_df, dimensions, [])

        self.assertEqual(
            {
                'columns': [
                    {'Header': '', 'accessor': '$metrics'},
                    {'Header': 'Democrat', 'accessor': 'Democrat', 'path_accessor': ['Democrat']},
                    {'Header': 'Independent', 'accessor': 'Independent', 'path_accessor': ['Independent']},
                    {'Header': 'Republican', 'accessor': 'Republican', 'path_accessor': ['Republican']},
                ],
                'data': [
                    {
                        '$metrics': {'raw': 'Wins'},
                        'Democrat': {'display': '6', 'raw': 6},
                        'Independent': {'display': '0', 'raw': 0},
                        'Republican': {'display': '6', 'raw': 6},
                    }
                ],
            },
            result,
        )

    def test_dim_with_hyperlink_depending_on_another_dim_not_included_if_other_dim_is_not_selected(self):
        result = ReactTable(mock_dataset.fields.wins).transform(dimx1_str_df, [mock_dataset.fields.political_party], [])

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$wins': {'display': '6', 'raw': 6},
                    },
                    {
                        '$political_party': {
                            'raw': 'Independent',
                            'hyperlink': 'http://example.com/Independent',
                        },
                        '$wins': {'display': '0', 'raw': 0},
                    },
                ],
            },
            result,
        )

    def test_dim_with_hyperlink_depending_on_another_dim_included_if_other_dim_is_selected(self):
        result = ReactTable(mock_dataset.fields.wins).transform(
            dimx2_str_str_df,
            [
                mock_dataset.fields.political_party,
                mock_dataset.fields['candidate-name'],
            ],
            [],
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Party', 'accessor': '$political_party'},
                    {'Header': 'Candidate Name', 'accessor': '$candidate-name'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$candidate-name': {
                            'hyperlink': 'http://example.com/Democrat/Al Gore',
                            'raw': 'Al Gore',
                        },
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$wins': {'display': '0', 'raw': 0},
                    },
                    {
                        '$candidate-name': {
                            'hyperlink': 'http://example.com/Democrat/Barrack Obama',
                            'raw': 'Barrack Obama',
                        },
                        '$political_party': {
                            'raw': 'Democrat',
                            'hyperlink': 'http://example.com/Democrat',
                        },
                        '$wins': {'display': '4', 'raw': 4},
                    },
                ],
            },
            result,
        )

    def test_dim_with_hyperlink_depending_on_another_dim_included_if_other_dim_is_selected_even_if_hidden(self):
        result = ReactTable(mock_dataset.fields.wins, hide=[mock_dataset.fields.political_party]).transform(
            dimx2_str_str_df,
            [
                mock_dataset.fields.political_party,
                mock_dataset.fields['candidate-name'],
            ],
            [],
        )

        self.assertIn('data', result)
        result['data'] = result['data'][:2]  # shorten the results to make the test easier to read

        self.assertEqual(
            {
                'columns': [
                    {'Header': 'Candidate Name', 'accessor': '$candidate-name'},
                    {'Header': 'Wins', 'accessor': '$wins', 'path_accessor': ['$wins']},
                ],
                'data': [
                    {
                        '$candidate-name': {
                            'hyperlink': 'http://example.com/Democrat/Al Gore',
                            'raw': 'Al Gore',
                        },
                        '$wins': {'display': '0', 'raw': 0},
                    },
                    {
                        '$candidate-name': {
                            'hyperlink': 'http://example.com/Democrat/Barrack Obama',
                            'raw': 'Barrack Obama',
                        },
                        '$wins': {'display': '4', 'raw': 4},
                    },
                ],
            },
            result,
        )


class ReactTableReferenceItemFormatTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ref_item_attrs = [
            'alias',
            'label',
            'prefix',
            'suffix',
            'thousands',
            'precision',
            'data_type',
        ]

    def assert_object_dict(self, obj, exp, attributes=()):
        for attribute in attributes:
            with self.subTest(attribute + ' should be equal'):
                self.assertEqual(getattr(obj, attribute), exp[attribute])

    def test_base_ref_item(self):
        exp_ref_item = {
            'alias': 'wins_with_style_eoe',
            'label': 'Wins EoE',
            'prefix': '$',
            'suffix': None,
            'thousands': '_',
            'precision': 0,
            'data_type': DataType.number,
        }

        ref = ElectionOverElection(mock_dataset.fields.timestamp)
        ref_item = ReferenceItem(mock_dataset.fields.wins_with_style, ref)

        self.assert_object_dict(ref_item, exp_ref_item, self.ref_item_attrs)

    def test_ref_item_with_delta_percentage_formats_prefix_suffix(self):
        exp_ref_item = {
            'alias': 'wins_with_style_eoe_delta_percent',
            'label': 'Wins EoE Δ%',
            'prefix': None,
            'suffix': '%',
            'thousands': '_',
            'precision': 0,
            'data_type': DataType.number,
        }

        ref = ElectionOverElection(mock_dataset.fields.timestamp, delta=True, delta_percent=True)
        ref_item = ReferenceItem(mock_dataset.fields.wins_with_style, ref)

        self.assert_object_dict(ref_item, exp_ref_item, self.ref_item_attrs)
