# coding: utf-8
import unittest
from collections import OrderedDict
from datetime import date

from fireant import settings
from fireant.slicer.queries import QueryManager
from fireant.tests.database.mock_database import TestDatabase
from pypika import Tables, functions as fn, JoinType


class QueryTests(unittest.TestCase):
    dao = QueryManager()
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
        query = self.dao._build_query(
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

                # Example using functions and Arithmetic
                ('ratio', fn.Sum(self.mock_table.numerator) / fn.Sum(self.mock_table.denominator)),
            ]),
            dimensions=OrderedDict([
                # Example of using a continuous datetime dimension, where the values are rounded up to the nearest day
                ('dt', settings.database.round_date(self.mock_table.dt, 'DD')),

                # Example of using a categorical dimension from a joined table
                ('fiz', self.mock_join2.fiz),
            ]),
            mfilters=[
                fn.Sum(self.mock_join2.buz) > 100
            ],
            dfilters=[
                # Example of filtering the query to a date range
                self.mock_table.dt[date(2016, 1, 1):date(2016, 12, 31)],

                # Example of filtering the query to certain categories
                self.mock_join2.fiz.isin(['a', 'b', 'c']),
            ],
            references=OrderedDict([
                # Example of adding a Week-over-Week comparison to the query
                ('wow', 'dt')
            ]),
            rollup=[],
        )

        self.assertEqual('SELECT '
                         # Dimensions
                         '"sq0"."dt" "dt","sq0"."fiz" "fiz",'
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
                         'ROUND("test_table"."dt",\'DD\') "dt","test_join2"."fiz" "fiz",'
                         'SUM("test_table"."foo") "foo",'
                         'AVG("test_join1"."bar") "bar",'
                         'SUM("test_table"."numerator")/SUM("test_table"."denominator") "ratio" '
                         'FROM "test_table" '
                         'JOIN "test_join1" ON "test_table"."join1_id"="test_join1"."id" '
                         'LEFT JOIN "test_join2" ON "test_table"."join2_id"="test_join2"."id" '
                         'WHERE "test_table"."dt" BETWEEN \'2016-01-01\' AND \'2016-12-31\' '
                         'AND "test_join2"."fiz" IN (\'a\',\'b\',\'c\') '
                         'GROUP BY ROUND("test_table"."dt",\'DD\'),"test_join2"."fiz" '
                         'HAVING SUM("test_join2"."buz")>100'
                         ') "sq0" '
                         'LEFT JOIN ('
                         # Reference Query
                         'SELECT '
                         'ROUND("test_table"."dt",\'DD\') "dt","test_join2"."fiz" "fiz",'
                         'SUM("test_table"."foo") "foo",'
                         'AVG("test_join1"."bar") "bar",'
                         'SUM("test_table"."numerator")/SUM("test_table"."denominator") "ratio" '
                         'FROM "test_table" '
                         'JOIN "test_join1" ON "test_table"."join1_id"="test_join1"."id" '
                         'LEFT JOIN "test_join2" ON "test_table"."join2_id"="test_join2"."id" '
                         'WHERE "test_table"."dt"+INTERVAL \'1 WEEK\' BETWEEN \'2016-01-01\' AND \'2016-12-31\' '
                         'AND "test_join2"."fiz" IN (\'a\',\'b\',\'c\') '
                         'GROUP BY ROUND("test_table"."dt",\'DD\'),"test_join2"."fiz" '
                         'HAVING SUM("test_join2"."buz")>100'
                         ') "sq1" ON "sq0"."dt"="sq1"."dt"+INTERVAL \'1 WEEK\' '
                         'AND "sq0"."fiz"="sq1"."fiz" '
                         'ORDER BY "sq0"."dt","sq0"."fiz"', str(query))


class MetricsTests(QueryTests):
    def test_metrics(self):
        query = self.dao._build_query(
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
        query = self.dao._build_query(
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
        query = self.dao._build_query(
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
        query = self.dao._build_query(
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
    def _test_rounded_timeseries(self, increment):
        rounded_dt = settings.database.round_date(self.mock_table.dt, increment)

        return self.dao._build_query(
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('dt', rounded_dt)
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

    def test_timeseries_hour(self):
        query = self._test_rounded_timeseries('HH')

        self.assertEqual(
            'SELECT ROUND("dt",\'HH\') "dt",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'HH\') '
            'ORDER BY ROUND("dt",\'HH\')', str(query))

    def test_timeseries_DD(self):
        query = self._test_rounded_timeseries('DD')

        self.assertEqual(
            'SELECT ROUND("dt",\'DD\') "dt",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'DD\') '
            'ORDER BY ROUND("dt",\'DD\')', str(query))

    def test_timeseries_week(self):
        query = self._test_rounded_timeseries('WW')

        self.assertEqual(
            'SELECT ROUND("dt",\'WW\') "dt",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'WW\') '
            'ORDER BY ROUND("dt",\'WW\')', str(query))

    def test_timeseries_month(self):
        query = self._test_rounded_timeseries('MONTH')

        self.assertEqual(
            'SELECT ROUND("dt",\'MONTH\') "dt",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'MONTH\') '
            'ORDER BY ROUND("dt",\'MONTH\')', str(query))

    def test_timeseries_quarter(self):
        query = self._test_rounded_timeseries('Q')

        self.assertEqual(
            'SELECT ROUND("dt",\'Q\') "dt",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'Q\') '
            'ORDER BY ROUND("dt",\'Q\')', str(query))

    def test_timeseries_year(self):
        query = self._test_rounded_timeseries('YEAR')

        self.assertEqual(
            'SELECT ROUND("dt",\'YEAR\') "dt",SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'YEAR\') '
            'ORDER BY ROUND("dt",\'YEAR\')', str(query))

    def test_multidimension_categorical(self):
        query = self.dao._build_query(
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
        rounded_dt = settings.database.round_date(self.mock_table.dt, 'DD')
        device_type = self.mock_table.device_type

        query = self.dao._build_query(
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('dt', rounded_dt),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual(
            'SELECT ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'GROUP BY ROUND("dt",\'DD\'),"device_type" '
            'ORDER BY ROUND("dt",\'DD\'),"device_type"', str(query))

    def test_metrics_with_joins(self):
        rounded_dt = settings.database.round_date(self.mock_table.dt, 'DD')
        locale = self.mock_table.locale

        query = self.dao._build_query(
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
                ('dt', rounded_dt),
                ('locale', locale),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=[],
        )

        self.assertEqual('SELECT '
                         'ROUND("test_table"."dt",\'DD\') "dt","test_table"."locale" "locale",'
                         'SUM("test_table"."clicks") "clicks",'
                         'SUM("test_table"."revenue")/SUM("test_table"."cost") "roi",'
                         '"test_join1"."hotel_name" "hotel_name","test_join1"."address" "hotel_address",'
                         '"test_join1"."ctid" "city_id","test_join1"."city_name" "city_name" '
                         'FROM "test_table" '
                         'LEFT JOIN "test_join1" ON "test_table"."hotel_id"="test_join1"."hotel_id" '
                         'GROUP BY ROUND("test_table"."dt",\'DD\'),"test_table"."locale" '
                         'ORDER BY ROUND("test_table"."dt",\'DD\'),"test_table"."locale"', str(query))


class FilterTests(QueryTests):
    def test_single_dimension_filter(self):
        query = self.dao._build_query(
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
        query = self.dao._build_query(
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
        query = self.dao._build_query(
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
        query = self.dao._build_query(
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


class ComparisonTests(QueryTests):
    intervals = {
        'yoy': '52 WEEK',
        'qoq': '1 QUARTER',
        'mom': '4 WEEK',
        'wow': '1 WEEK',
    }

    def _get_compare_query(self, compare_type):
        rounded_dt = settings.database.round_date(self.mock_table.dt, 'DD')
        device_type = self.mock_table.device_type
        query = self.dao._build_query(
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('dt', rounded_dt),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[
                self.mock_table.dt[date(2000, 1, 1):date(2000, 3, 1)]
            ],
            references=OrderedDict([
                (compare_type, 'dt')
            ]),
            rollup=[],
        )
        return query

    def assert_reference(self, query, key):
        self.assertEqual(
            'SELECT '
            '"sq0"."dt" "dt","sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            # '"sq1"."dt" "dt_{key}",'
            '"sq1"."clicks" "clicks_{key}",'
            '"sq1"."roi" "roi_{key}" '
            'FROM ('
            'SELECT '
            'ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY ROUND("dt",\'DD\'),"device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            'ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL \'{expr}\' BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY ROUND("dt",\'DD\'),"device_type"'
            ') "sq1" ON "sq0"."dt"="sq1"."dt"+INTERVAL \'{expr}\' '
            'AND "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."dt","sq0"."device_type"'.format(
                key=key,
                expr=self.intervals[key]
            ), str(query)
        )

    def assert_reference_d(self, query, key):
        self.assertEqual(
            'SELECT '
            '"sq0"."dt" "dt","sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            # '"sq1"."dt" "dt_{key}_d",'
            '"sq0"."clicks"-"sq1"."clicks" "clicks_{key}_d",'
            '"sq0"."roi"-"sq1"."roi" "roi_{key}_d" '
            'FROM ('
            'SELECT '
            'ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY ROUND("dt",\'DD\'),"device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            'ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL \'{expr}\' BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY ROUND("dt",\'DD\'),"device_type"'
            ') "sq1" ON "sq0"."dt"="sq1"."dt"+INTERVAL \'{expr}\' '
            'AND "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."dt","sq0"."device_type"'.format(
                key=key,
                expr=self.intervals[key]
            ), str(query)
        )

    def assert_reference_p(self, query, key):
        self.assertEqual(
            'SELECT '
            '"sq0"."dt" "dt","sq0"."device_type" "device_type",'
            '"sq0"."clicks" "clicks","sq0"."roi" "roi",'
            # '"sq1"."dt" "dt_{key}_p",'
            '("sq0"."clicks"-"sq1"."clicks")/"sq1"."clicks" "clicks_{key}_p",'
            '("sq0"."roi"-"sq1"."roi")/"sq1"."roi" "roi_{key}_p" '
            'FROM ('
            'SELECT '
            'ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY ROUND("dt",\'DD\'),"device_type"'
            ') "sq0" '
            'LEFT JOIN ('
            'SELECT '
            'ROUND("dt",\'DD\') "dt","device_type" "device_type",'
            'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
            'FROM "test_table" '
            'WHERE "dt"+INTERVAL \'{expr}\' BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
            'GROUP BY ROUND("dt",\'DD\'),"device_type"'
            ') "sq1" ON "sq0"."dt"="sq1"."dt"+INTERVAL \'{expr}\' '
            'AND "sq0"."device_type"="sq1"."device_type" '
            'ORDER BY "sq0"."dt","sq0"."device_type"'.format(
                key=key,
                expr=self.intervals[key]
            ), str(query)
        )

    def test_metrics_dimensions_filters_references__yoy(self):
        query = self._get_compare_query('yoy')
        self.assert_reference(query, 'yoy')

    def test_metrics_dimensions_filters_references__qoq(self):
        query = self._get_compare_query('qoq')
        self.assert_reference(query, 'qoq')

    def test_metrics_dimensions_filters_references__mom(self):
        query = self._get_compare_query('mom')
        self.assert_reference(query, 'mom')

    def test_metrics_dimensions_filters_references__wow(self):
        query = self._get_compare_query('wow')
        self.assert_reference(query, 'wow')

    def test_metrics_dimensions_filters_references__yoy_d(self):
        query = self._get_compare_query('yoy_d')
        self.assert_reference_d(query, 'yoy')

    def test_metrics_dimensions_filters_references__qoq_d(self):
        query = self._get_compare_query('qoq_d')
        self.assert_reference_d(query, 'qoq')

    def test_metrics_dimensions_filters_references__mom_d(self):
        query = self._get_compare_query('mom_d')
        self.assert_reference_d(query, 'mom')

    def test_metrics_dimensions_filters_references__wow_d(self):
        query = self._get_compare_query('wow_d')
        self.assert_reference_d(query, 'wow')

    def test_metrics_dimensions_filters_references__yoy_p(self):
        query = self._get_compare_query('yoy_p')
        self.assert_reference_p(query, 'yoy')

    def test_metrics_dimensions_filters_references__qoq_p(self):
        query = self._get_compare_query('qoq_p')
        self.assert_reference_p(query, 'qoq')

    def test_metrics_dimensions_filters_references__mom_p(self):
        query = self._get_compare_query('mom_p')
        self.assert_reference_p(query, 'mom')

    def test_metrics_dimensions_filters_references__wow_p(self):
        query = self._get_compare_query('wow_p')
        self.assert_reference_p(query, 'wow')


class TotalsQueryTests(QueryTests):
    def test_add_rollup_one_dimension(self):
        rounded_dt = settings.database.round_date(self.mock_table.dt, 'DD')
        locale = self.mock_table.locale

        query = self.dao._build_query(
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('dt', rounded_dt),
                ('locale', locale),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=['locale'],
        )

        self.assertEqual('SELECT '
                         'ROUND("dt",\'DD\') "dt","locale" "locale",'
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY ROUND("dt",\'DD\'),ROLLUP("locale") '
                         'ORDER BY ROUND("dt",\'DD\'),"locale"', str(query))

    def test_add_rollup_two_dimensions(self):
        rounded_dt = settings.database.round_date(self.mock_table.dt, 'DD')
        locale = self.mock_table.locale
        device_type = self.mock_table.device_type

        query = self.dao._build_query(
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('dt', rounded_dt),
                ('locale', locale),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=['locale', 'device_type'],
        )

        self.assertEqual('SELECT '
                         'ROUND("dt",\'DD\') "dt",'
                         '"locale" "locale",'
                         '"device_type" "device_type",'
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY ROUND("dt",\'DD\'),ROLLUP("locale","device_type") '
                         'ORDER BY ROUND("dt",\'DD\'),"locale","device_type"', str(query))

    def test_add_rollup_two_dimensions_partial(self):
        rounded_dt = settings.database.round_date(self.mock_table.dt, 'DD')
        locale = self.mock_table.locale
        device_type = self.mock_table.device_type

        query = self.dao._build_query(
            table=self.mock_table,
            joins=[],
            metrics=OrderedDict([
                ('clicks', fn.Sum(self.mock_table.clicks)),
                ('roi', fn.Sum(self.mock_table.revenue) / fn.Sum(self.mock_table.cost)),
            ]),
            dimensions=OrderedDict([
                ('dt', rounded_dt),
                ('locale', locale),
                ('device_type', device_type),
            ]),
            mfilters=[],
            dfilters=[],
            references={},
            rollup=['locale'],
        )
        self.assertEqual('SELECT '
                         'ROUND("dt",\'DD\') "dt",'
                         '"device_type" "device_type",'
                         '"locale" "locale",'  # Order is changed, rollup dims move to end
                         'SUM("clicks") "clicks",SUM("revenue")/SUM("cost") "roi" '
                         'FROM "test_table" '
                         'GROUP BY ROUND("dt",\'DD\'),"device_type",ROLLUP("locale") '
                         'ORDER BY ROUND("dt",\'DD\'),"locale","device_type"', str(query))
