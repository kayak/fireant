# coding: utf-8
import unittest
from collections import OrderedDict
from datetime import date

from mock import patch
from pypika import Tables, functions as fn, JoinType

from fireant import settings
from fireant.database import MySQLDatabase
from fireant.slicer import references
from fireant.slicer.queries import QueryManager, QueryNotSupportedError
from fireant.tests.database.mock_database import TestDatabase


class QueryTests(unittest.TestCase):
    manager = QueryManager(database=TestDatabase())
    maxDiff = None

    mock_table, mock_join1, mock_join2 = Tables('test_table', 'test_join1', 'test_join2')

    @classmethod
    def setUpClass(cls):
        settings.database = TestDatabase()


class ExampleTests(QueryTests):
    def test_full_example(self):
        """
        This is an example using several features of the slicer.  It demonstrates usage of metrics, dimensions, filters,
        references and also joining metrics from additional tables.

        The _build_query function takes a dictionary parameter that expresses all of the options of the slicer aside
        from Operations which are done in a post-processing step.

        Many of the fields are defined as a dict.  In the examples and other tests an OrderedDict is used in many places
        so that the field order is maintained for the assertions.  In a real example, a regular dict can be used.

        The fields in the dictionary are as follows:

        :param 'table':
            The primary table to query data from.  This is the table used for the FROM clause in the query.
        :param 'joins':
            A list or tuple of tuples.  This lists the tables that need to be joined in the query and the criterion to
            use to join them.  The inner tuples must contain two elements, the first element is the table to join and
            the second element is a criterion to join the tables with.
        :param 'metrics':
            A dict containing the SELECT clause.  The values can be strings (as a short cut for a column of the primary
            table), a field instance, or an expression containing functions, arithmetic, or anything else supported by
            pypika.
        :param 'dimensions':
            A dict containing the INDEX of the query.  These fields will be included in the SELECT clause, the GROUP BY
            clause, and the ORDER BY clause.  For comparisons, they are also used to join the nested query.
        :param 'filters':
            A list containing criterion expressions for the WHERE clause.  Multiple filters will be combined with an
            'AND' operator.
        :param 'references':
            A dict containing comparison operators.  The keys of this dict must match a supported comparison operation.
            The value of the key must match the key from the dimensions table.  The dimension must also be of the
            supported type, for example 'yoy' requires a DATE type dimension.
        :param 'rollup':
            A list of dimensions to rollup, or provide the totals across groups.  When multiple dimensions are included,
            rollup works from the last to the first dimension, providing the totals across the dimensions in a tree
            structure.

        See pypika documentation for more examples of query expressions: http://pypika.readthedocs.io/en/latest/
        """
        dt = self.mock_table.dt
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[
                (self.mock_join1, self.mock_table.join1_id == self.mock_join1.id, JoinType.inner),
                (self.mock_join2, self.mock_table.join2_id == self.mock_join2.id, JoinType.left),
            ],
            metrics=OrderedDict([
                # Examples using a field of a table
                ('foo', fn.Sum(self.mock_table.foo)),

                # Examples using a field of a table
                ('bar', fn.Avg(self.mock_join1.bar)),

                # Example using functions and Arithmetic
                ('ratio', fn.Sum(self.mock_table.numerator) / fn.Sum(self.mock_table.denominator)),
            ]),
            dimensions=OrderedDict([
                # Example of using a continuous datetime dimension, where the values are truncated to the nearest day
                ('date', settings.database.trunc_date(dt, 'day')),

                # Example of using a categorical dimension from a joined table
                ('fiz', self.mock_join2.fiz),
            ]),
            mfilters=[
                fn.Sum(self.mock_join2.buz) > 100
            ],
            dfilters=[
                # Example of filtering the query to a date range
                dt[date(2016, 1, 1):date(2016, 12, 31)],

                # Example of filtering the query to certain categories
                self.mock_join2.fiz.isin(['a', 'b', 'c']),
            ],
            references=OrderedDict([
                # Example of adding a Week-over-Week comparison to the query
                (references.WoW.key, {
                    'dimension': 'date', 'definition': dt,
                    'interval': references.WoW.interval,
                })
            ]),
            rollup=[],
        )

        self.assertEqual('SELECT '
                         # Dimensions
                         '"sq0"."date" "date","sq0"."fiz" "fiz",'
                         # Metrics
                         '"sq0"."foo" "foo","sq0"."bar" "bar","sq0"."ratio" "ratio",'
                         # Reference Dimension
                         # Currently not selected
                         # '"sq1"."dt" "dt_wow",'
                         # Reference Metrics
                         '"sq1"."foo" "foo_wow","sq1"."bar" "bar_wow","sq1"."ratio" "ratio_wow" '
                         'FROM ('
                         # Main Query
                         'SELECT '
                         'TRUNC("test_table"."dt",\'DD\') "date","test_join2"."fiz" "fiz",'
                         'SUM("test_table"."foo") "foo",'
                         'AVG("test_join1"."bar") "bar",'
                         'SUM("test_table"."numerator")/SUM("test_table"."denominator") "ratio" '
                         'FROM "test_table" '
                         'JOIN "test_join1" ON "test_table"."join1_id"="test_join1"."id" '
                         'LEFT JOIN "test_join2" ON "test_table"."join2_id"="test_join2"."id" '
                         'WHERE "test_table"."dt" BETWEEN \'2016-01-01\' AND \'2016-12-31\' '
                         'AND "test_join2"."fiz" IN (\'a\',\'b\',\'c\') '
                         'GROUP BY TRUNC("test_table"."dt",\'DD\'),"test_join2"."fiz" '
                         'HAVING SUM("test_join2"."buz")>100'
                         ') "sq0" '
                         'LEFT JOIN ('
                         # Reference Query
                         'SELECT '
                         'TRUNC("test_table"."dt",\'DD\') "date","test_join2"."fiz" "fiz",'
                         'SUM("test_table"."foo") "foo",'
                         'AVG("test_join1"."bar") "bar",'
                         'SUM("test_table"."numerator")/SUM("test_table"."denominator") "ratio" '
                         'FROM "test_table" '
                         'JOIN "test_join1" ON "test_table"."join1_id"="test_join1"."id" '
                         'LEFT JOIN "test_join2" ON "test_table"."join2_id"="test_join2"."id" '
                         'WHERE "test_table"."dt"+INTERVAL \'1 WEEK\' BETWEEN \'2016-01-01\' AND \'2016-12-31\' '
                         'AND "test_join2"."fiz" IN (\'a\',\'b\',\'c\') '
                         'GROUP BY TRUNC("test_table"."dt",\'DD\'),"test_join2"."fiz" '
                         'HAVING SUM("test_join2"."buz")>100'
                         ') "sq1" ON "sq0"."date"="sq1"."date"+INTERVAL \'1 WEEK\' '
                         'AND "sq0"."fiz"="sq1"."fiz" '
                         'ORDER BY "sq0"."date","sq0"."fiz"', str(query))


class MetricsTests(QueryTests):
    def test_metrics(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions={},
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table"', str(query))

    def test_metrics_dimensions(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('device_type', self.mock_table.device_type)
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual(
            'SELECT "device_type" "device_type",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY "device_type" '
            'ORDER BY "device_type"', str(query))

    def test_metrics_filters(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('device_type', self.mock_table.device_type)
            ]),
            mfilters=[],
            dfilters=[
                self.mock_table.dt[date(2000, 1, 1):date(2001, 1, 1)]
            ],
            references={},
            rollup=[],
        )

        self.assertEqual(
            'SELECT "device_type" "device_type",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2001-01-01\' '
            'GROUP BY "device_type" '
            'ORDER BY "device_type"', str(query))

    def test_metrics_dimensions_filters(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', (fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost))),
            ]),
            dimensions=OrderedDict([
                ('device_type', self.mock_table.device_type),
                ('locale', self.mock_table.locale),
            ]),
            mfilters=[],
            dfilters=[
                self.mock_table.locale.isin(['US', 'CA', 'UK'])
            ],
            references={},
            rollup=[],
        )

        self.assertEqual(
            'SELECT '
            '"device_type" "device_type",'
            '"locale" "locale",'
            'SUM("clicks") "clicks",'
            'SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "locale" IN (\'US\',\'CA\',\'UK\') '
            'GROUP BY "device_type","locale" '
            'ORDER BY "device_type","locale"', str(query))


class DimensionTests(QueryTests):
    def _test_truncated_timeseries(self, increment):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, increment)

        return self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt)
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

    def test_timeseries_hour(self):
        query = self._test_truncated_timeseries('hour')

        self.assertEqual(
            'SELECT TRUNC("dt",\'HH\') "date",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'HH\') '
            'ORDER BY TRUNC("dt",\'HH\')', str(query))

    def test_timeseries_DD(self):
        query = self._test_truncated_timeseries('day')

        self.assertEqual(
            'SELECT TRUNC("dt",\'DD\') "date",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'DD\') '
            'ORDER BY TRUNC("dt",\'DD\')', str(query))

    def test_timeseries_week(self):
        query = self._test_truncated_timeseries('week')

        self.assertEqual(
            'SELECT TRUNC("dt",\'IW\') "date",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'IW\') '
            'ORDER BY TRUNC("dt",\'IW\')', str(query))

    def test_timeseries_month(self):
        query = self._test_truncated_timeseries('month')

        self.assertEqual(
            'SELECT TRUNC("dt",\'MM\') "date",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'MM\') '
            'ORDER BY TRUNC("dt",\'MM\')', str(query))

    def test_timeseries_quarter(self):
        query = self._test_truncated_timeseries('quarter')

        self.assertEqual(
            'SELECT TRUNC("dt",\'Q\') "date",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'Q\') '
            'ORDER BY TRUNC("dt",\'Q\')', str(query))

    def test_timeseries_year(self):
        query = self._test_truncated_timeseries('year')

        self.assertEqual(
            'SELECT TRUNC("dt",\'Y\') "date",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'Y\') '
            'ORDER BY TRUNC("dt",\'Y\')', str(query))

    def test_multidimension_categorical(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('device_type', self.mock_table.device_type),
                ('locale', self.mock_table.locale),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual(
            'SELECT "device_type" "device_type","locale" "locale",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY "device_type","locale" '
            'ORDER BY "device_type","locale"', str(query))

    def test_multidimension_timeseries_categorical(self):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, 'day')
        device_type = self.mock_table.device_type

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual(
            'SELECT TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type" '
            'ORDER BY TRUNC("dt",\'DD\'),"device_type"', str(query))

    def test_metrics_with_joins(self):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, 'day')
        locale = self.mock_table.locale

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[
                (self.mock_join1, self.mock_table.hotel_id == self.mock_join1.hotel_id, JoinType.left),
            ],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
                ('hotel_name', self.mock_join1.hotel_name),
                ('hotel_address', self.mock_join1.address),
                ('city_id', self.mock_join1.ctid),
                ('city_name', self.mock_join1.city_name),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt),
                ('locale', locale),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT '
                         'TRUNC("test_table"."dt",\'DD\') "date","test_table"."locale" "locale",'
                         'SUM("test_table"."clicks") "clicks",'
                         'SUM("test_table"."revenue")/SUM("test_table"."cost") "roi",'
                         '"test_join1"."hotel_name" "hotel_name","test_join1"."address" "hotel_address",'
                         '"test_join1"."ctid" "city_id","test_join1"."city_name" "city_name" '
                         'FROM "test_table" '
                         'LEFT JOIN "test_join1" ON "test_table"."hotel_id"="test_join1"."hotel_id" '
                         'GROUP BY TRUNC("test_table"."dt",\'DD\'),"test_table"."locale" '
                         'ORDER BY TRUNC("test_table"."dt",\'DD\'),"test_table"."locale"', str(query))


class FilterTests(QueryTests):
    def test_single_dimension_filter(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', (fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost))),
            ]),
            dimensions=OrderedDict([
                ('locale', self.mock_table.locale),
            ]),
            mfilters=[],
            dfilters=[
                self.mock_table.locale.isin(['US', 'CA', 'UK'])
            ],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT '
                         '"locale" "locale",'
                         'SUM("clicks") "clicks",'
                         'SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'WHERE "locale" IN (\'US\',\'CA\',\'UK\') '
                         'GROUP BY "locale" '
                         'ORDER BY "locale"', str(query))

    def test_multi_dimension_filter(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', (fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost))),
            ]),
            dimensions=OrderedDict([
                ('locale', self.mock_table.locale),
            ]),
            mfilters=[],
            dfilters=[
                self.mock_table.locale.isin(['US', 'CA', 'UK']),
                self.mock_table.device_type == 'desktop',
                self.mock_table.dt > date(2016, 1, 1),
            ],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT '
                         '"locale" "locale",'
                         'SUM("clicks") "clicks",'
                         'SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'WHERE "locale" IN (\'US\',\'CA\',\'UK\') '
                         'AND "device_type"=\'desktop\' '
                         'AND "dt">\'2016-01-01\' '
                         'GROUP BY "locale" '
                         'ORDER BY "locale"', str(query))

    def test_single_metric_filter(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', (fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost))),
            ]),
            dimensions=OrderedDict([
                ('locale', self.mock_table.locale),
            ]),
            mfilters=[
                fn.Sum(self.mock_table.clicks) > 100
            ],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT '
                         '"locale" "locale",'
                         'SUM("clicks") "clicks",'
                         'SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY "locale" '
                         'HAVING SUM("clicks")>100 '
                         'ORDER BY "locale"', str(query))

    def test_multi_metric_filter(self):
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', (fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost))),
            ]),
            dimensions=OrderedDict([
                ('locale', self.mock_table.locale),
            ]),
            mfilters=[
                fn.Sum(self.mock_table.clicks) > 100,
                (fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)) < 0.7,
                fn.Sum(self.mock_table.conversions) >= 10,
            ],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT '
                         '"locale" "locale",'
                         'SUM("clicks") "clicks",'
                         'SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY "locale" '
                         'HAVING SUM("clicks")>100 '
                         'AND SUM("revenue")/SUM("cost")<0.7 '
                         'AND SUM("conversions")>=10 '
                         'ORDER BY "locale"', str(query))


class ReferenceTests(QueryTests):
    intervals = {
        'yoy': '1',
        'qoq': '1 QUARTER',
        'mom': '1 MONTH',
        'wow': '1 WEEK',
        'dod': '1 DAY',
    }

    def _get_compare_query(self, ref):
        dt = self.mock_table.dt
        device_type = self.mock_table.device_type
        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', settings.database.trunc_date(dt, 'day')),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[
                dt[date(2000, 1, 1):date(2000, 3, 1)]
            ],
            references=OrderedDict([
                (ref.key, {'dimension': ref.element_key, 'definition': dt,
                           'interval': ref.interval, 'modifier': ref.modifier})
            ]),
            rollup=[],
        )
        return query

    def assert_reference(self, query, key, interval=None):
        interval = interval if interval else '\'{expr}\''.format(expr=self.intervals[key])

        self.assertEqual(
            'SELECT '
            '"sq0"."date" "date","sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            '"sq1"."clicks" "clicks_{key}",'
            '"sq1"."roi" "roi_{key}" '
            'FROM ('
            'SELECT '
            'TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            'TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL {interval} BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type"'
            ') "sq1" ON "sq0"."date"="sq1"."date"+INTERVAL {interval} '
            'AND "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."date","sq0"."device_type"'.format(
                key=key,
                interval=interval
            ), str(query)
        )

    def assert_reference_d(self, query, key, interval=None):
        interval = interval if interval else '\'{expr}\''.format(expr=self.intervals[key])

        self.assertEqual(
            'SELECT '
            '"sq0"."date" "date","sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            '"sq0"."clicks"-"sq1"."clicks" "clicks_{key}_delta",'
            '"sq0"."roi"-"sq1"."roi" "roi_{key}_delta" '
            'FROM ('
            'SELECT '
            'TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            'TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL {interval} BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type"'
            ') "sq1" ON "sq0"."date"="sq1"."date"+INTERVAL {interval} '
            'AND "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."date","sq0"."device_type"'.format(
                key=key,
                interval=interval
            ), str(query)
        )

    def assert_reference_p(self, query, key, interval=None):
        interval = interval if interval else '\'{expr}\''.format(expr=self.intervals[key])

        self.assertEqual(
            'SELECT '
            '"sq0"."date" "date","sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            '("sq0"."clicks"-"sq1"."clicks")*100/NULLIF("sq1"."clicks",0) "clicks_{key}_delta_percent",'
            '("sq0"."roi"-"sq1"."roi")*100/NULLIF("sq1"."roi",0) "roi_{key}_delta_percent" '
            'FROM ('
            'SELECT '
            'TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            'TRUNC("dt",\'DD\') "date","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL {interval} BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY TRUNC("dt",\'DD\'),"device_type"'
            ') "sq1" ON "sq0"."date"="sq1"."date"+INTERVAL {interval} '
            'AND "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."date","sq0"."device_type"'.format(
                key=key,
                interval=interval
            ), str(query)
        )

    def test_metrics_dimensions_filters_references__yoy(self):
        reference = references.YoY('date')
        query = self._get_compare_query(reference)
        self.assert_reference(query, reference.key, '\'{expr}\' YEAR'.format(expr=self.intervals[reference.key]))

    def test_metrics_dimensions_filters_references__qoq(self):
        reference = references.QoQ('date')
        query = self._get_compare_query(reference)
        self.assert_reference(query, reference.key)

    def test_metrics_dimensions_filters_references__mom(self):
        reference = references.MoM('date')
        query = self._get_compare_query(reference)
        self.assert_reference(query, reference.key)

    def test_metrics_dimensions_filters_references__wow(self):
        reference = references.WoW('date')
        query = self._get_compare_query(reference)
        self.assert_reference(query, reference.key)

    def test_metrics_dimensions_filters_references__dod(self):
        reference = references.DoD('date')
        query = self._get_compare_query(reference)
        self.assert_reference(query, reference.key)

    def test_metrics_dimensions_filters_references__yoy_d(self):
        reference = references.YoY('date')
        query = self._get_compare_query(references.Delta(reference))
        self.assert_reference_d(query, reference.key, '\'{expr}\' YEAR'.format(expr=self.intervals[reference.key]))

    def test_metrics_dimensions_filters_references__qoq_d(self):
        reference = references.QoQ('date')
        query = self._get_compare_query(references.Delta(reference))
        self.assert_reference_d(query, reference.key)

    def test_metrics_dimensions_filters_references__mom_d(self):
        reference = references.MoM('date')
        query = self._get_compare_query(references.Delta(reference))
        self.assert_reference_d(query, reference.key)

    def test_metrics_dimensions_filters_references__wow_d(self):
        reference = references.WoW('date')
        query = self._get_compare_query(references.Delta(reference))
        self.assert_reference_d(query, reference.key)

    def test_metrics_dimensions_filters_references__dod_d(self):
        reference = references.DoD('date')
        query = self._get_compare_query(references.Delta(reference))
        self.assert_reference_d(query, reference.key)

    def test_metrics_dimensions_filters_references__yoy_p(self):
        reference = references.YoY('date')
        query = self._get_compare_query(references.DeltaPercentage(reference))
        self.assert_reference_p(query, reference.key, '\'{expr}\' YEAR'.format(expr=self.intervals[reference.key]))

    def test_metrics_dimensions_filters_references__qoq_p(self):
        reference = references.QoQ('date')
        query = self._get_compare_query(references.DeltaPercentage(reference))
        self.assert_reference_p(query, reference.key)

    def test_metrics_dimensions_filters_references__mom_p(self):
        reference = references.MoM('date')
        query = self._get_compare_query(references.DeltaPercentage(reference))
        self.assert_reference_p(query, reference.key)

    def test_metrics_dimensions_filters_references__wow_p(self):
        reference = references.WoW('date')
        query = self._get_compare_query(references.DeltaPercentage(reference))
        self.assert_reference_p(query, reference.key)

    def test_metrics_dimensions_filters_references__dod_p(self):
        reference = references.DoD('date')
        query = self._get_compare_query(references.DeltaPercentage(reference))
        self.assert_reference_p(query, reference.key)

    def test_metrics_dimensions_filters_references__no_date_dimension(self):
        ref = references.DoD('date')
        dt = self.mock_table.dt
        device_type = self.mock_table.device_type

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                # NO Date Dimension
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[
                dt[date(2000, 1, 1):date(2000, 3, 1)]
            ],
            references=OrderedDict([
                (ref.key, {'dimension': ref.element_key, 'definition': self.mock_table.dt,
                           'interval': ref.interval, 'modifier': ref.modifier})
            ]),
            rollup=[],
        )

        self.assertEqual(
            'SELECT '
            '"sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            '"sq1"."clicks" "clicks_{key}",'
            '"sq1"."roi" "roi_{key}" '
            'FROM ('
            'SELECT '
            '"device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY "device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            '"device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL \'{expr}\' BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY "device_type"'
            ') "sq1" ON "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."device_type"'.format(
                key=ref.key,
                expr=self.intervals[ref.key]
            ), str(query)
        )

    def test_metrics_dimensions_filters_references__no_dimensions(self):
        ref = references.DoD('date')
        dt = self.mock_table.dt

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions={},
            mfilters=[],
            dfilters=[
                dt[date(2000, 1, 1):date(2000, 3, 1)]
            ],
            references=OrderedDict([
                (ref.key, {'dimension': ref.element_key, 'definition': self.mock_table.dt,
                           'interval': ref.interval, 'modifier': ref.modifier})
            ]),
            rollup=[],
        )

        self.assertEqual(
            'SELECT '
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            '"sq1"."clicks" "clicks_{key}",'
            '"sq1"."roi" "roi_{key}" '
            'FROM ('
            'SELECT '
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\''
            ') "sq0",('
            'SELECT '
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL \'{expr}\' BETWEEN \'2000-01-01\' AND \'2000-03-01\''
            ') "sq1"'.format(
                key=ref.key,
                expr=self.intervals[ref.key]
            ), str(query)
        )


class TotalsQueryTests(QueryTests):
    def test_add_rollup_one_dimension(self):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, 'day')
        locale = self.mock_table.locale

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt),
                ('locale', locale),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[['locale']],
        )

        self.assertEqual('SELECT '
                         'TRUNC("dt",\'DD\') "date","locale" "locale",'
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY TRUNC("dt",\'DD\'),ROLLUP(("locale")) '
                         'ORDER BY TRUNC("dt",\'DD\'),"locale"', str(query))

    def test_add_rollup_two_dimensions(self):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, 'day')
        locale = self.mock_table.locale
        device_type = self.mock_table.device_type

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt),
                ('locale', locale),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[['locale'], ['device_type']],
        )

        self.assertEqual('SELECT '
                         'TRUNC("dt",\'DD\') "date",'
                         '"locale" "locale",'
                         '"device_type" "device_type",'
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY TRUNC("dt",\'DD\'),ROLLUP(("locale"),("device_type")) '
                         'ORDER BY TRUNC("dt",\'DD\'),"locale","device_type"', str(query))

    def test_add_rollup_two_dimensions_partial(self):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, 'day')
        locale = self.mock_table.locale
        device_type = self.mock_table.device_type

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt),
                ('locale', locale),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[['locale']],
        )
        self.assertEqual('SELECT '
                         'TRUNC("dt",\'DD\') "date",'
                         '"device_type" "device_type",'
                         '"locale" "locale",'  # Order is changed, rollup dims move to end
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY TRUNC("dt",\'DD\'),"device_type",ROLLUP(("locale")) '
                         'ORDER BY TRUNC("dt",\'DD\'),"locale","device_type"', str(query))

    def test_add_rollup_uni_dimension(self):
        truncated_dt = settings.database.trunc_date(self.mock_table.dt, 'day')
        locale = self.mock_table.locale
        locale_display = self.mock_table.locale_display
        device_type = self.mock_table.device_type

        query = self.manager._build_data_query(
            database=settings.database,
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('date', truncated_dt),
                ('locale', locale),
                ('locale_display', locale_display),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[['locale', 'locale_display']],
        )
        self.assertEqual('SELECT '
                         'TRUNC("dt",\'DD\') "date",'
                         '"locale" "locale",'
                         '"locale_display" "locale_display",'
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY TRUNC("dt",\'DD\'),ROLLUP(("locale","locale_display")) '
                         'ORDER BY TRUNC("dt",\'DD\'),"locale","locale_display"', str(query))


class DimensionOptionTests(QueryTests):
    def test_dimension_options(self):
        locale = self.mock_table.locale

        query = self.manager._build_dimension_query(
            table=self.mock_table,
            joins=[],
            dimensions=OrderedDict([
                ('locale', locale),
            ]),
            filters=[],
        )

        self.assertEqual('SELECT DISTINCT '
                         '"locale" "locale" '
                         'FROM "test_table"', str(query))

    def test_dimension_options_with_query_with_limit(self):
        locale = self.mock_table.locale

        query = self.manager._build_dimension_query(
            table=self.mock_table,
            joins=[],
            dimensions=OrderedDict([
                ('locale', locale),
            ]),
            filters=[],
            limit=10,
        )

        self.assertEqual('SELECT DISTINCT '
                         '"locale" "locale" '
                         'FROM "test_table" '
                         'LIMIT 10', str(query))

    def test_dimension_options_with_query_with_filter(self):
        locale = self.mock_table.locale

        query = self.manager._build_dimension_query(
            table=self.mock_table,
            joins=[],
            dimensions=OrderedDict([
                ('locale', locale),
            ]),
            filters=[
                self.mock_table.device_type == 'desktop',
            ],
        )

        self.assertEqual('SELECT DISTINCT '
                         '"locale" "locale" '
                         'FROM "test_table" '
                         'WHERE "device_type"=\'desktop\'', str(query))

    def test_dimension_options_with_multiple_dimensions(self):
        account_id = self.mock_table.account_id
        account_name = self.mock_table.account_name

        query = self.manager._build_dimension_query(
            table=self.mock_table,
            joins=[],
            dimensions=OrderedDict([
                ('account_id', account_id),
                ('account_name', account_name),
            ]),
            filters=[],
            limit=10
        )

        self.assertEqual('SELECT DISTINCT '
                         '"account_id" "account_id",'
                         '"account_name" "account_name" '
                         'FROM "test_table" '
                         'LIMIT 10', str(query))

    def test_dimension_options_with_joins(self):
        account_id = self.mock_table.account_id

        query = self.manager._build_dimension_query(
            table=self.mock_table,
            joins=[
                (self.mock_join1, account_id == self.mock_join1.account_id, JoinType.left),
            ],
            dimensions=OrderedDict([
                ('account_id', account_id),
                ('account_name', self.mock_join1.account_name),
            ]),
            filters=[],
        )

        self.assertEqual('SELECT DISTINCT '
                         '"test_table"."account_id" "account_id",'
                         '"test_join1"."account_name" "account_name" '
                         'FROM "test_table" '
                         'LEFT JOIN "test_join1" '
                         'ON "test_table"."account_id"="test_join1"."account_id"', str(query))

    @patch.object(MySQLDatabase, 'fetch_dataframe')
    def test_exception_raised_if_rollup_requested_for_a_mysql_database(self, mock_db):
        db = MySQLDatabase(database='testdb')
        manager = QueryManager(database=db)

        with self.assertRaises(QueryNotSupportedError):
            manager.query_data(db, self.mock_table, rollup=[['locale']])
