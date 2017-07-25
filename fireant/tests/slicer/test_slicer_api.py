# coding: utf-8
from unittest import TestCase

import numpy as np
import pandas as pd
from datetime import date

from fireant.slicer import *
from fireant.slicer.operations import *
from fireant.slicer.references import *
from fireant.tests.database.mock_database import TestDatabase
from pypika import JoinType
from pypika import functions as fn, Tables, Case

QUERY_BUILDER_PARAMS = {'table', 'database', 'joins', 'metrics', 'dimensions', 'mfilters', 'dfilters', 'references',
                        'rollup', 'pagination'}


class SlicerSchemaTests(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.test_table, cls.test_join_table = Tables('test_table', 'test_join_table')
        cls.test_table.alias = 'test'
        cls.test_join_table.alias = 'join'
        cls.test_db = TestDatabase()

        cls.test_slicer = Slicer(
            table=cls.test_table,
            database=cls.test_db,

            joins=[
                Join('join1', cls.test_join_table, cls.test_table.join_id == cls.test_join_table.id)
            ],

            metrics=[
                # Metric with defaults
                Metric('foo'),

                # Metric with custom label and definition
                Metric('bar', label='FizBuz', definition=fn.Sum(cls.test_table.fiz + cls.test_table.buz)),

                # Metric with complex definition
                Metric('weirdcase', label='Weird Case',
                       definition=(
                           Case().when(cls.test_table.case == 1, 'a')
                               .when(cls.test_table.case == 2, 'b')
                               .else_('weird'))),

                # Metric with joins
                Metric('piddle', definition=fn.Sum(cls.test_join_table.piddle), joins=['join1']),

                # Metric with custom label and definition
                Metric('paddle', definition=fn.Sum(cls.test_join_table.paddle + cls.test_table.foo), joins=['join1']),

                # Metric with rounding
                Metric('decimal', precision=2),

                # Metric with prefix
                Metric('dollar', prefix='$'),

                # Metric with suffix
                Metric('euro', suffix='€'),

                # Metric with suffix
                Metric('join_metric', definition=fn.Sum(cls.test_join_table.join_metric), joins=['join1']),
            ],

            dimensions=[
                # Continuous date dimension
                DatetimeDimension('date', definition=cls.test_table.dt),

                # Continuous integer dimension
                ContinuousDimension('clicks', label='My Clicks', definition=cls.test_table.clicks),

                # Categorical dimension with display options
                CategoricalDimension('locale', 'Locale', definition=cls.test_table.locale,
                                     display_options=[DimensionValue('us', 'United States'),
                                                      DimensionValue('de', 'Germany')]),

                # Unique Dimension with single ID field
                UniqueDimension('account', 'Account', definition=cls.test_table.account_id,
                                display_field=cls.test_table.account_name),

                # Unique Dimension with composite ID field
                UniqueDimension('keyword', 'Keyword', definition=cls.test_table.keyword_id,
                                display_field=cls.test_table.keyword_name),

                # Dimension with joined columns
                UniqueDimension('join_dimension', 'Join Dimension', definition=cls.test_join_table.join_dimension,
                                display_field=cls.test_join_table.join_dimension_display, joins=['join1']),
            ]
        )


class SlicerSchemaMetricTests(SlicerSchemaTests):
    def test_metric_with_default_definition(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

    def test_metric_with_custom_definition(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['bar'],
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'bar'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."fiz"+"test"."buz")', str(query_schema['metrics']['bar']))

    def test_metrics_added_for_cumsum(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            operations=[CumSum('foo', )]
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))

    def test_metrics_added_for_cummean(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            operations=[CumMean('foo')]
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))

    def test_metrics_added_for_l1loss(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            operations=[L1Loss('foo', 'bar')]
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo', 'bar'}, set(query_schema['metrics'].keys()))

    def test_metrics_added_for_l2loss(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=[],
            operations=[L2Loss('foo', 'bar')]
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo', 'bar'}, set(query_schema['metrics'].keys()))


class SlicerSchemaDimensionTests(SlicerSchemaTests):
    def test_date_dimension_default_interval(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['date'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'DD\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_custom_week_interval(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            # TODO This could be improved by using an object
            dimensions=[('date', DatetimeDimension.week)],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'IW\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_year_interval_uses_correct_trunc_statement(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=[('date', DatetimeDimension.year)],
        )
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'Y\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_quarter_interval_uses_correct_trunc_statement(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=[('date', DatetimeDimension.quarter)],
        )
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'Q\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_month_interval_uses_correct_trunc_statement(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=[('date', DatetimeDimension.month)],
        )
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'MM\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_day_interval_uses_correct_trunc_statement(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=[('date', DatetimeDimension.day)],
        )
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'DD\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_hour_interval_uses_correct_trunc_statement(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=[('date', DatetimeDimension.hour)],
        )
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'HH\')', str(query_schema['dimensions']['date']))

    def test_numeric_dimension_default_interval(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['clicks'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'clicks'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('MOD("test"."clicks"+0,1)', str(query_schema['dimensions']['clicks']))

    def test_numeric_dimension_custom_interval(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            # TODO This could be improved by using an object
            dimensions=[('clicks', 100, 25)],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'clicks'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('MOD("test"."clicks"+25,100)', str(query_schema['dimensions']['clicks']))

    def test_categorical_dimension(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

    def test_unique_dimension_id(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['account'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'account', 'account_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."account_id"', str(query_schema['dimensions']['account']))
        self.assertEqual('"test"."account_name"', str(query_schema['dimensions']['account_display']))

    def test_multiple_metrics_and_dimensions(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo', 'bar'],
            dimensions=[('date', DatetimeDimension.month), ('clicks', 50, 100), 'locale', 'account'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo', 'bar'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))
        self.assertEqual('SUM("test"."fiz"+"test"."buz")', str(query_schema['metrics']['bar']))

        self.assertSetEqual({'date', 'clicks', 'locale', 'account', 'account_display'},
                            set(query_schema['dimensions'].keys()))
        self.assertEqual('MOD("test"."clicks"+100,50)', str(query_schema['dimensions']['clicks']))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))
        self.assertEqual('"test"."account_id"', str(query_schema['dimensions']['account']))
        self.assertEqual('"test"."account_name"', str(query_schema['dimensions']['account_display']))


class SlicerSchemaFilterTests(SlicerSchemaTests):
    def test_cat_dimension_filter_eq(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[EqualityFilter('locale', EqualityOperator.eq, 'en')],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."locale"=\'en\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_ne(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('locale', EqualityOperator.ne, 'en'),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."locale"<>\'en\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_in(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                ContainsFilter('locale', ['en', 'es', 'de']),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."locale" IN (\'en\',\'es\',\'de\')'], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_like(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                WildcardFilter('locale', 'e%'),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."locale" LIKE \'e%\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_gt(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.gt, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."dt">\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_lt(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.lt, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."dt"<\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_gte(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.gte, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."dt">=\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_lte(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.lte, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"test"."dt"<=\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_cat_dimension_filter_daterange(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                RangeFilter('date', date(2000, 1, 1), date(2000, 3, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertListEqual(['"test"."dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\''],
                             [str(f) for f in query_schema['dfilters']])

    def test_unique_dimension_eq_filter(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['account'],
            dimension_filters=[EqualityFilter('account', EqualityOperator.eq, 1)],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'account', 'account_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."account_id"', str(query_schema['dimensions']['account']))
        self.assertEqual('"test"."account_name"', str(query_schema['dimensions']['account_display']))

        self.assertListEqual(['"test"."account_id"=1'], [str(f) for f in query_schema['dfilters']])

    def test_unique_dimension_display_eq_filter(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['account'],
            dimension_filters=[EqualityFilter(('account', 'display'), EqualityOperator.eq, 'abc')],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'account', 'account_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."account_id"', str(query_schema['dimensions']['account']))
        self.assertEqual('"test"."account_name"', str(query_schema['dimensions']['account_display']))

        self.assertListEqual(['"test"."account_name"=\'abc\''], [str(f) for f in query_schema['dfilters']])

    def test_unique_dimension_contains_filter(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['account'],
            dimension_filters=[ContainsFilter('account', [1, 2, 3])],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'account', 'account_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."account_id"', str(query_schema['dimensions']['account']))
        self.assertEqual('"test"."account_name"', str(query_schema['dimensions']['account_display']))

        self.assertListEqual(['"test"."account_id" IN (1,2,3)'], [str(f) for f in query_schema['dfilters']])

    def test_unique_dimension_wildcard_filter_label(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['account'],
            dimension_filters=[WildcardFilter(('account', 'display'), 'nam%')],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'account', 'account_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."account_id"', str(query_schema['dimensions']['account']))
        self.assertEqual('"test"."account_name"', str(query_schema['dimensions']['account_display']))

        self.assertListEqual(['"test"."account_name" LIKE \'nam%\''], [str(f) for f in query_schema['dfilters']])

    def test_metric_filter_eq(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[EqualityFilter('foo', EqualityOperator.eq, 0)],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("test"."foo")=0'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_ne(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.ne, 0),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("test"."foo")<>0'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_gt(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.gt, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("test"."foo")>100'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_lt(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.lt, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("test"."foo")<100'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_gte(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.gte, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("test"."foo")>=100'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_lte(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.lte, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"test"."locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("test"."foo")<=100'], [str(f) for f in query_schema['mfilters']])

    def test_invalid_dimensions_raise_exception(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.data_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                dimension_filters=[
                    EqualityFilter('blahblahblah', EqualityOperator.eq, 0),
                ],
            )

    def test_invalid_metrics_raise_exception(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.data_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                metric_filters=[
                    EqualityFilter('blahblahblah', EqualityOperator.eq, 0),
                ],
            )

    def test_metrics_dont_work_for_dimensions(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.data_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                dimension_filters=[
                    EqualityFilter('foo', EqualityOperator.gt, 100),
                ],
            )

    def test_dimensions_dont_work_for_metrics(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.data_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                metric_filters=[
                    EqualityFilter('locale', EqualityOperator.eq, 'US'),
                ],
            )

    def test_joined_metric_filter(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            metric_filters=[
                EqualityFilter('join_metric', EqualityOperator.eq, 0),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertSetEqual(set(), set(query_schema['dimensions'].keys()))

        self.assertEqual(1, len(query_schema['joins']))
        self.assertEqual(3, len(query_schema['joins'][0]))
        self.assertEqual(self.test_join_table, query_schema['joins'][0][0])
        self.assertEqual('"test"."join_id"="join"."id"', str(query_schema['joins'][0][1]))
        self.assertEqual(JoinType.inner, query_schema['joins'][0][2])

    def test_joined_dimension_filter(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimension_filters=[
                EqualityFilter('join_dimension', EqualityOperator.eq, 'test'),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertSetEqual(set(), set(query_schema['dimensions'].keys()))

        self.assertEqual(1, len(query_schema['joins']))
        self.assertEqual(3, len(query_schema['joins'][0]))
        self.assertEqual(self.test_join_table, query_schema['joins'][0][0])
        self.assertEqual('"test"."join_id"="join"."id"', str(query_schema['joins'][0][1]))
        self.assertEqual(JoinType.inner, query_schema['joins'][0][2])

    def test_joined_unique_dimension_display_filter(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimension_filters=[
                EqualityFilter(('join_dimension', 'display'), EqualityOperator.eq, 'test'),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertSetEqual(set(), set(query_schema['dimensions'].keys()))

        self.assertEqual(1, len(query_schema['joins']))
        self.assertEqual(3, len(query_schema['joins'][0]))
        self.assertEqual(self.test_join_table, query_schema['joins'][0][0])
        self.assertEqual('"test"."join_id"="join"."id"', str(query_schema['joins'][0][1]))
        self.assertEqual(JoinType.inner, query_schema['joins'][0][2])


class SlicerSchemaReferenceTests(SlicerSchemaTests):
    def _reference_test_with_date(self, reference):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['date'],
            references=[reference],
        )
        self.assertTrue({'table', 'metrics', 'dimensions', 'references'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])
        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'DD\')', str(query_schema['dimensions']['date']))

        # Cast definition to string for comparison
        query_schema['references'][reference.key]['definition'] = str(
            query_schema['references'][reference.key]['definition']
        )

        self.assertDictEqual({
            reference.key: {
                'dimension': reference.element_key,
                'definition': '"test"."dt"',
                'interval': reference.interval,
                'modifier': reference.modifier,
            }
        }, query_schema['references'])

    def test_reference_wow_with_date(self):
        self._reference_test_with_date(WoW('date'))

    def test_reference_wow_d_with_date(self):
        self._reference_test_with_date(Delta(WoW('date')))

    def test_reference_wow_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage(WoW('date')))

    def test_reference_mom_with_date(self):
        self._reference_test_with_date(MoM('date'))

    def test_reference_mom_d_with_date(self):
        self._reference_test_with_date(Delta(MoM('date')))

    def test_reference_mom_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage(MoM('date')))

    def test_reference_qoq_with_date(self):
        self._reference_test_with_date(QoQ('date'))

    def test_reference_qoq_d_with_date(self):
        self._reference_test_with_date(Delta(QoQ('date')))

    def test_reference_qoq_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage(QoQ('date')))

    def test_reference_yoy_with_date(self):
        self._reference_test_with_date(YoY('date'))

    def test_reference_yoy_d_with_date(self):
        self._reference_test_with_date(Delta(YoY('date')))

    def test_reference_yoy_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage(YoY('date')))

    def test_reference_missing_dimension(self):
        # Throws exception when reference key is not a dimension key
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.data_query_schema(
                metrics=['foo'],
                dimensions=[],
                references=[WoW('blahblah')],
            )

    def test_reference_wrong_dimension_type(self):
        # Reference dimension is required in order to use a reference with it
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.data_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                references=[WoW('locale')],
            )


class SlicerOperationSchemaTests(SlicerSchemaTests):
    def test_totals_query_schema(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['date', 'locale', 'account'],
            operations=[Totals('locale', 'account')],
        )

        self.assertTrue({'table', 'metrics', 'dimensions', 'rollup'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'date', 'locale', 'account', 'account_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('TRUNC("test"."dt",\'DD\')', str(query_schema['dimensions']['date']))

        self.assertListEqual([['locale'], ['account', 'account_display']], query_schema['rollup'])

    def test_totals_operation_schema(self):
        operation_schema = self.test_slicer.manager.operation_schema(
            operations=[Totals('locale', 'account')],
        )

        self.assertListEqual([], operation_schema)

    def test_cumsum_operation_schema(self):
        operation_schema = self.test_slicer.manager.operation_schema(
            operations=[CumSum('foo')],
        )

        self.assertListEqual([{'key': 'cumsum', 'metric': 'foo'}], operation_schema)

    def test_cummean_operation_schema(self):
        operation_schema = self.test_slicer.manager.operation_schema(
            operations=[CumMean('foo')],
        )

        self.assertListEqual([{'key': 'cummean', 'metric': 'foo'}], operation_schema)

    def test_l1loss_operation_schema(self):
        operation_schema = self.test_slicer.manager.operation_schema(
            operations=[L1Loss('foo', 'bar')],
        )

        self.assertListEqual([{'key': 'l1loss', 'metric': 'foo', 'target': 'bar'}], operation_schema)

    def test_l2loss_operation_schema(self):
        operation_schema = self.test_slicer.manager.operation_schema(
            operations=[L2Loss('foo', 'bar')],
        )

        self.assertListEqual([{'key': 'l2loss', 'metric': 'foo', 'target': 'bar'}], operation_schema)


class SlicerSchemaJoinTests(SlicerSchemaTests):
    def test_metric_with_join(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['piddle'],
        )

        self.assertTrue({'table', 'metrics', 'joins'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        join1 = query_schema['joins'][0]
        self.assertEqual(self.test_join_table, join1[0])
        self.assertEqual('"test"."join_id"="join"."id"', str(join1[1]))

        self.assertSetEqual({'piddle'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("join"."piddle")', str(query_schema['metrics']['piddle']))

    def test_metric_with_complex_join(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['paddle'],
        )

        self.assertTrue({'table', 'metrics', 'joins'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        join1 = query_schema['joins'][0]
        self.assertEqual(self.test_join_table, join1[0])
        self.assertEqual('"test"."join_id"="join"."id"', str(join1[1]))

        self.assertSetEqual({'paddle'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("join"."paddle"+"test"."foo")', str(query_schema['metrics']['paddle']))

    def test_dimension_with_join(self):
        query_schema = self.test_slicer.manager.data_query_schema(
            metrics=['foo'],
            dimensions=['join_dimension'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions', 'joins'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("test"."foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'join_dimension', 'join_dimension_display'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"join"."join_dimension"', str(query_schema['dimensions']['join_dimension']))
        self.assertEqual('"join"."join_dimension_display"', str(query_schema['dimensions']['join_dimension_display']))


class SlicerDisplaySchemaTests(SlicerSchemaTests):
    def test_metric_with_default_definition(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': {'label': 'foo', 'axis': 0}},
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_metric_with_custom_definition(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['bar'],
        )

        self.assertDictEqual(
            {
                'metrics': {'bar': {'label': 'FizBuz', 'axis': 0}},
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_date_dimension_default_interval(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
            dimensions=['date'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': {'label': 'foo', 'axis': 0}},
                'dimensions': {
                    'date': {'label': 'date'}
                },
                'references': {},
            },
            display_schema
        )

    def test_numeric_dimension_default_interval(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
            dimensions=['clicks'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': {'label': 'foo', 'axis': 0}},
                'dimensions': {
                    'clicks': {'label': 'My Clicks'}
                },
                'references': {},
            },
            display_schema
        )

    def test_categorical_dimension(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
            dimensions=['locale'],
        )
        self.assertDictEqual(
            {
                'metrics': {'foo': {'label': 'foo', 'axis': 0}},
                'dimensions': {
                    'locale': {'label': 'Locale', 'display_options': {
                        'us': 'United States', 'de': 'Germany', np.nan: '', pd.NaT: ''
                    }},
                },
                'references': {},
            },
            display_schema
        )

    def test_unique_dimension(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
            dimensions=['account'],
        )
        self.assertDictEqual(
            {
                'metrics': {'foo': {'label': 'foo', 'axis': 0}},
                'dimensions': {
                    'account': {'label': 'Account', 'display_field': 'account_display'},
                },
                'references': {},
            },
            display_schema
        )

    def test_multiple_metrics_and_dimensions(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo', 'bar'],
            dimensions=[('date', DatetimeDimension.month), ('clicks', 50, 100), 'locale', 'account'],
        )

        self.assertDictEqual(
            {
                'metrics': {
                    'foo': {'label': 'foo', 'axis': 0},
                    'bar': {'label': 'FizBuz', 'axis': 1},
                },
                'dimensions': {
                    'date': {'label': 'date'},
                    'clicks': {'label': 'My Clicks'},
                    'locale': {'label': 'Locale', 'display_options': {
                        'us': 'United States', 'de': 'Germany', np.nan: '', pd.NaT: ''
                    }},
                    'account': {'label': 'Account', 'display_field': 'account_display'},
                },
                'references': {},
            },
            display_schema
        )

    def test_reference_wow_with_date(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
            dimensions=['date'],
            references=[WoW('date')],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': {'label': 'foo', 'axis': 0}},
                'dimensions': {
                    'date': {'label': 'date'},
                },
                'references': {'wow': 'WoW'},
            },
            display_schema
        )

    def test_cumsum_operation(self):
        display_schema = self.test_slicer.manager.display_schema(
            operations=[CumSum('foo')],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo_cumsum': {'label': 'foo cum. sum', 'axis': 0}},
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_cummean_operation(self):
        display_schema = self.test_slicer.manager.display_schema(
            operations=[CumMean('foo')],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo_cummean': {'label': 'foo cum. mean', 'axis': 0}},
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_l1loss_operation(self):
        display_schema = self.test_slicer.manager.display_schema(
            operations=[L1Loss('foo', 'bar')],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo_l1loss': {'label': 'foo L1 loss', 'axis': 0}},
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_l2loss_operation(self):
        display_schema = self.test_slicer.manager.display_schema(
            operations=[L2Loss('foo', 'bar')],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo_l2loss': {'label': 'foo L2 loss', 'axis': 0}},
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_operation_with_metric(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['bar'],
            operations=[CumSum('foo')],
        )

        self.assertDictEqual(
            {
                'metrics': {
                    'bar': {'label': 'FizBuz', 'axis': 0},
                    'foo_cumsum': {'label': 'foo cum. sum', 'axis': 1}
                },
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_operation_with_same_metric(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['foo'],
            operations=[CumSum('foo')],
        )

        self.assertDictEqual(
            {
                'metrics': {
                    'foo': {'label': 'foo', 'axis': 0},
                    'foo_cumsum': {'label': 'foo cum. sum', 'axis': 1}
                },
                'dimensions': {},
                'references': {},
            },
            display_schema
        )

    def test_metric_with_rounding(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['decimal'],
            dimensions=['date'],
        )

        self.assertDictEqual(
            {
                'metrics': {
                    'decimal': {'label': 'decimal', 'axis': 0, 'precision': 2},
                },
                'dimensions': {
                    'date': {'label': 'date'}
                },
                'references': {},
            },
            display_schema
        )

    def test_metric_with_prefix(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['dollar'],
            dimensions=['date'],
        )

        self.assertDictEqual(
            {
                'metrics': {
                    'dollar': {'label': 'dollar', 'axis': 0, 'prefix': '$'},
                },
                'dimensions': {
                    'date': {'label': 'date'}
                },
                'references': {},
            },
            display_schema
        )

    def test_metric_with_suffix(self):
        display_schema = self.test_slicer.manager.display_schema(
            metrics=['euro'],
            dimensions=['date'],
        )

        self.assertDictEqual(
            {
                'metrics': {
                    'euro': {'label': 'euro', 'axis': 0, 'suffix': '€'},
                },
                'dimensions': {
                    'date': {'label': 'date'}
                },
                'references': {},
            },
            display_schema
        )
