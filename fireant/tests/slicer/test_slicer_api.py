# coding: utf-8
from datetime import date
from unittest import TestCase, skip

from pypika import functions as fn, Table, Case

from fireant import settings
from fireant.slicer import (EqualityOperator, EqualityFilter, RangeFilter, WildcardFilter, ContainsFilter, Metric,
                            DatetimeDimension, Slicer, CategoricalDimension, DimensionValue, UniqueDimension,
                            ContinuousDimension, Rollup, WoW, MoM, QoQ, YoY, Delta, DeltaPercentage, SlicerException)
from fireant.tests.database.mock_database import TestDatabase

QUERY_BUILDER_PARAMS = {'table', 'joins', 'metrics', 'dimensions', 'mfilters', 'dfilters', 'references', 'rollup'}


class SlicerSchemaTests(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        settings.database = TestDatabase()
        cls.test_table = Table('test_table')

        cls.test_slicer = Slicer(
            cls.test_table,

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
                               .else_('weird')))
            ],

            dimensions=[
                # Continuous date dimension
                DatetimeDimension('date', definition=cls.test_table.dt),

                # Continuous integer dimension
                ContinuousDimension('clicks', label='Clicks CUSTOM LABEL', definition=cls.test_table.clicks),

                # Categorical dimension with fixed options
                CategoricalDimension('locale', 'Locale', definition=cls.test_table.locale,
                                     options=[DimensionValue('us', 'United States'),
                                              DimensionValue('de', 'Germany')]),

                # Unique Dimension with single ID field
                UniqueDimension('account', 'Account',
                                label_field=cls.test_table.account_name,
                                id_fields=[cls.test_table.account_id]),

                # Unique Dimension with composite ID field
                UniqueDimension('keyword', 'Keyword',
                                label_field=cls.test_table.keyword_name,
                                id_fields=[cls.test_table.keyword_id, cls.test_table.keyword_type,
                                           cls.test_table.adgroup_id, cls.test_table.engine]),
            ]
        )


class SlicerSchemaMetricTests(SlicerSchemaTests):
    def test_metric_with_default_definition(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

    def test_metric_with_custom_definition(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['bar'],
        )

        self.assertTrue({'table', 'metrics'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'bar'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("fiz"+"buz")', str(query_schema['metrics']['bar']))


class SlicerSchemaDimensionTests(SlicerSchemaTests):
    def test_date_dimension_default_interval(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['date'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('ROUND("dt",\'DD\')', str(query_schema['dimensions']['date']))

    def test_date_dimension_custom_interval(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            # TODO This could be improved by using an object
            dimensions=[('date', DatetimeDimension.week)],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('ROUND("dt",\'WW\')', str(query_schema['dimensions']['date']))

    def test_numeric_dimension_default_interval(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['clicks'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'clicks'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('MOD("clicks"+0,1)', str(query_schema['dimensions']['clicks']))

    def test_numeric_dimension_custom_interval(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            # TODO This could be improved by using an object
            dimensions=[('clicks', 100, 25)],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'clicks'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('MOD("clicks"+25,100)', str(query_schema['dimensions']['clicks']))

    def test_categorical_dimension(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

    def test_unique_dimension_single_id(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['account'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'account_id0', 'account_label'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"account_id"', str(query_schema['dimensions']['account_id0']))
        self.assertEqual('"account_name"', str(query_schema['dimensions']['account_label']))

    def test_unique_dimension_composite_id(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['keyword'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'keyword_id0', 'keyword_id1', 'keyword_id2', 'keyword_id3', 'keyword_label'},
                            set(query_schema['dimensions'].keys()))
        self.assertEqual('"keyword_id"', str(query_schema['dimensions']['keyword_id0']))
        self.assertEqual('"keyword_type"', str(query_schema['dimensions']['keyword_id1']))
        self.assertEqual('"adgroup_id"', str(query_schema['dimensions']['keyword_id2']))
        self.assertEqual('"engine"', str(query_schema['dimensions']['keyword_id3']))
        self.assertEqual('"keyword_name"', str(query_schema['dimensions']['keyword_label']))

    def test_multiple_metrics_and_dimensions(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo', 'bar'],
            dimensions=[('date', DatetimeDimension.month), ('clicks', 50, 100), 'locale', 'account'],
        )

        self.assertTrue({'table', 'metrics', 'dimensions'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo', 'bar'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))
        self.assertEqual('SUM("fiz"+"buz")', str(query_schema['metrics']['bar']))

        self.assertSetEqual({'date', 'clicks', 'locale', 'account_id0', 'account_label'},
                            set(query_schema['dimensions'].keys()))
        self.assertEqual('MOD("clicks"+100,50)', str(query_schema['dimensions']['clicks']))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))
        self.assertEqual('"account_id"', str(query_schema['dimensions']['account_id0']))
        self.assertEqual('"account_name"', str(query_schema['dimensions']['account_label']))


class SlicerSchemaFilterTests(SlicerSchemaTests):
    def test_dimension_filter_eq(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[EqualityFilter('locale', EqualityOperator.eq, 'en')],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"locale"=\'en\''], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_ne(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('locale', EqualityOperator.ne, 'en'),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"locale"<>\'en\''], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_in(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                ContainsFilter('locale', ['en', 'es', 'de']),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"locale" IN (\'en\',\'es\',\'de\')'], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_like(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                WildcardFilter('locale', 'e%'),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"locale" LIKE \'e%\''], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_gt(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.gt, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"dt">\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_lt(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.lt, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"dt"<\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_gte(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.gte, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"dt">=\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_lte(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                EqualityFilter('date', EqualityOperator.lte, date(2000, 1, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['"dt"<=\'2000-01-01\''], [str(f) for f in query_schema['dfilters']])

    @skip('Need to decide how this should work')
    def test_dimension_filter_numericrange(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                RangeFilter('clicks', 25, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['MOD("clicks"+0,1) BETWEEN 25 AND 100'], [str(f) for f in query_schema['dfilters']])

    def test_dimension_filter_daterange(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            dimension_filters=[
                RangeFilter('date', date(2000, 1, 1), date(2000, 3, 1)),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertListEqual(['"dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\''],
                             [str(f) for f in query_schema['dfilters']])

    def test_metric_filter_eq(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[EqualityFilter('foo', EqualityOperator.eq, 0)],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("foo")=0'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_ne(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.ne, 0),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("foo")<>0'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_gt(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.gt, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("foo")>100'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_lt(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.lt, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("foo")<100'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_gte(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.gte, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("foo")>=100'], [str(f) for f in query_schema['mfilters']])

    def test_metric_filter_lte(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['locale'],
            metric_filters=[
                EqualityFilter('foo', EqualityOperator.lte, 100),
            ],
        )

        self.assertSetEqual(QUERY_BUILDER_PARAMS, set(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'locale'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('"locale"', str(query_schema['dimensions']['locale']))

        self.assertListEqual(['SUM("foo")<=100'], [str(f) for f in query_schema['mfilters']])

    def test_invalid_dimensions_raise_exception(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.get_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                dimension_filters=[
                    EqualityFilter('blahblahblah', EqualityOperator.eq, 0),
                ],
            )

    def test_invalid_metrics_raise_exception(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.get_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                metric_filters=[
                    EqualityFilter('blahblahblah', EqualityOperator.eq, 0),
                ],
            )

    def test_metrics_dont_work_for_dimensions(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.get_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                dimension_filters=[
                    EqualityFilter('foo', EqualityOperator.gt, 100),
                ],
            )

    def test_dimensions_dont_work_for_metrics(self):
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.get_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                metric_filters=[
                    EqualityFilter('locale', EqualityOperator.eq, 'US'),
                ],
            )


class SlicerSchemaReferenceTests(SlicerSchemaTests):
    def _reference_test_with_date(self, reference):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['date'],
            references=[reference],
        )
        self.assertTrue({'table', 'metrics', 'dimensions', 'references'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])
        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))
        self.assertSetEqual({'date'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('ROUND("dt",\'DD\')', str(query_schema['dimensions']['date']))
        self.assertListEqual([(reference.key, reference.element_key)], query_schema['references'])

    def test_reference_wow_with_date(self):
        self._reference_test_with_date(WoW('date'))

    def test_reference_wow_d_with_date(self):
        self._reference_test_with_date(Delta.WoW('date'))

    def test_reference_wow_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage.WoW('date'))

    def test_reference_mom_with_date(self):
        self._reference_test_with_date(MoM('date'))

    def test_reference_mom_d_with_date(self):
        self._reference_test_with_date(Delta.MoM('date'))

    def test_reference_mom_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage.MoM('date'))

    def test_reference_qoq_with_date(self):
        self._reference_test_with_date(QoQ('date'))

    def test_reference_qoq_d_with_date(self):
        self._reference_test_with_date(Delta.QoQ('date'))

    def test_reference_qoq_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage.QoQ('date'))

    def test_reference_yoy_with_date(self):
        self._reference_test_with_date(YoY('date'))

    def test_reference_yoy_d_with_date(self):
        self._reference_test_with_date(Delta.YoY('date'))

    def test_reference_yoy_p_with_date(self):
        self._reference_test_with_date(DeltaPercentage.YoY('date'))

    def test_reference_missing_dimension(self):
        # Reference dimension is required in order to use a reference with it
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.get_query_schema(
                metrics=['foo'],
                dimensions=[],
                references=[WoW('date')],
            )

    def test_reference_wrong_dimension_type(self):
        # Reference dimension is required in order to use a reference with it
        with self.assertRaises(SlicerException):
            self.test_slicer.manager.get_query_schema(
                metrics=['foo'],
                dimensions=['locale'],
                references=[WoW('locale')],
            )

    def test_rollup_operation(self):
        query_schema = self.test_slicer.manager.get_query_schema(
            metrics=['foo'],
            dimensions=['date', 'locale', 'account'],
            operations=[Rollup(['locale', 'account'])],
        )

        self.assertTrue({'table', 'metrics', 'dimensions', 'rollup'}.issubset(query_schema.keys()))
        self.assertEqual(self.test_table, query_schema['table'])

        self.assertSetEqual({'foo'}, set(query_schema['metrics'].keys()))
        self.assertEqual('SUM("foo")', str(query_schema['metrics']['foo']))

        self.assertSetEqual({'date', 'locale', 'account_id0', 'account_label'}, set(query_schema['dimensions'].keys()))
        self.assertEqual('ROUND("dt",\'DD\')', str(query_schema['dimensions']['date']))

        self.assertListEqual(['locale', 'account_id0', 'account_label'], query_schema['rollup'])


class SlicerDisplaySchemaTests(SlicerSchemaTests):
    def test_metric_with_default_definition(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [],
                'references': [],
            },
            display_schema
        )

    def test_metric_with_custom_definition(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['bar'],
        )

        self.assertDictEqual(
            {
                'metrics': {'bar': 'FizBuz'},
                'dimensions': [],
                'references': [],
            },
            display_schema
        )

    def test_date_dimension_default_interval(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
            dimensions=['date'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [
                    {'label': 'Date',
                     'id_fields': ['date']},
                ],
                'references': [],
            },
            display_schema
        )

    def test_numeric_dimension_default_interval(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
            dimensions=['clicks'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [
                    {'label': 'Clicks CUSTOM LABEL',
                     'id_fields': ['clicks']},
                ],
                'references': [],
            },
            display_schema
        )

    def test_categorical_dimension(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
            dimensions=['locale'],
        )
        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [
                    {'label': 'Locale',
                     'id_fields': ['locale'],
                     'label_options': {'us': 'United States',
                                       'de': 'Germany'}},
                ],
                'references': [],
            },
            display_schema
        )

    def test_unique_dimension_single_id(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
            dimensions=['account'],
        )
        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [
                    {'label': 'Account',
                     'id_fields': ['account_id0'],
                     'label_field': 'account_label'},
                ],
                'references': [],
            },
            display_schema
        )

    def test_unique_dimension_composite_id(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
            dimensions=['keyword'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [
                    {'label': 'Keyword',
                     'id_fields': ['keyword_id0', 'keyword_id1', 'keyword_id2', 'keyword_id3'],
                     'label_field': 'keyword_label'},
                ],
                'references': [],
            },
            display_schema
        )

    def test_multiple_metrics_and_dimensions(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo', 'bar'],
            dimensions=[('date', DatetimeDimension.month), ('clicks', 50, 100), 'locale', 'account'],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo', 'bar': 'FizBuz'},
                'dimensions': [
                    {'label': 'Date',
                     'id_fields': ['date']},
                    {'label': 'Clicks CUSTOM LABEL',
                     'id_fields': ['clicks']},
                    {'label': 'Locale',
                     'id_fields': ['locale'],
                     'label_options': {'us': 'United States', 'de': 'Germany'}},
                    {'label': 'Account',
                     'id_fields': ['account_id0'],
                     'label_field': 'account_label'},
                ],
                'references': [],
            },
            display_schema
        )

    def test_reference_wow_with_date(self):
        display_schema = self.test_slicer.manager.get_display_schema(
            metrics=['foo'],
            dimensions=['date'],
            references=[WoW('date')],
        )

        self.assertDictEqual(
            {
                'metrics': {'foo': 'Foo'},
                'dimensions': [
                    {'label': 'Date',
                     'id_fields': ['date']},
                ],
                'references': ['wow'],
            },
            display_schema
        )
