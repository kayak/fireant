from datetime import (
    date,
    datetime,
)
from unittest import TestCase

import pytz
from pypika import (
    Table,
    functions as fn,
)

import fireant as f
from fireant import DataSetFilterException
from fireant.tests.dataset.mocks import test_database

test_table = Table("test")
ds = f.DataSet(
    table=test_table,
    database=test_database,
    fields=[
        f.Field("date", definition=test_table.date, data_type=f.DataType.date),
        f.Field("text", definition=test_table.text, data_type=f.DataType.text),
        f.Field("number", definition=test_table.number, data_type=f.DataType.number),
        f.Field("boolean", definition=test_table.boolean, data_type=f.DataType.boolean),
        f.Field(
            "aggr_number",
            definition=fn.Sum(test_table.number),
            data_type=f.DataType.number,
        ),
    ],
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class FilterDateFieldTests(TestCase):
    maxDiff = None

    def test_eq_expr_str(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date == "2019-03-06").sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"='2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_eq_expr_date(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date == date(2019, 3, 6)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"='2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_eq_expr_datetime(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.date == datetime(2019, 3, 6, 9, 36, 11))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"='2019-03-06T09:36:11' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_eq_expr_datetime_timezone(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.date == datetime(2019, 3, 6, 9, 36, 11, tzinfo=pytz.UTC))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"='2019-03-06T09:36:11+00:00' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_ne_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date != date(2019, 3, 6)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"<>'2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_gt_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date > date(2019, 3, 6)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\">'2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_ge_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date >= date(2019, 3, 6)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\">='2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_lt_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date < date(2019, 3, 6)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"<'2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_le_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date <= date(2019, 3, 6)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"<='2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_in_expr(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.date.isin((date(2019, 3, 6), date(2019, 3, 7))))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\" IN ('2019-03-06','2019-03-07') "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_notin_expr(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.date.notin((date(2019, 3, 6), date(2019, 3, 7))))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\" NOT IN ('2019-03-06','2019-03-07') "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_between_expr(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.date.between(date(2019, 3, 6), date(2019, 3, 7)))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\" BETWEEN '2019-03-06' AND '2019-03-07' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.date.like("%stuff%")

    def test_not_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.date.not_like("%stuff%")

    def test_is_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.date.is_(True)

    def test_void_filter_with_no_other_filters(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.date.void()).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual('SELECT SUM("number") "$aggr_number" FROM "test" ORDER BY 1 LIMIT 200000', str(queries[0]))

    def test_void_filter_with_a_dimension_filter(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.date == "2019-03-06")
            .filter(ds.fields.date.void())
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"date\"='2019-03-06' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_void_filter_with_a_metric_filter(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(ds.fields.aggr_number > 10)
            .filter(ds.fields.date.void())
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")>10 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class FilterNumberFieldTests(TestCase):
    def test_eq_expr_int(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number == 1).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            'SELECT SUM("number") "$aggr_number" FROM "test" WHERE "number"=1 ORDER BY 1 LIMIT 200000',
            str(queries[0]),
        )

    def test_eq_expr_float(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number == 1.0).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT " 'SUM("number") "$aggr_number" ' 'FROM "test" ' 'WHERE "number"=1.0 ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_ne_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number != 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT " 'SUM("number") "$aggr_number" ' 'FROM "test" ' 'WHERE "number"<>5 ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_gt_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number > 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            'SELECT SUM("number") "$aggr_number" FROM "test" ' 'WHERE "number">5 ORDER BY 1 LIMIT 200000',
            str(queries[0]),
        )

    def test_ge_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number >= 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT " 'SUM("number") "$aggr_number" ' 'FROM "test" ' 'WHERE "number">=5 ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_lt_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number < 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            'SELECT SUM("number") "$aggr_number" FROM "test" WHERE "number"<5 ORDER BY 1 LIMIT 200000',
            str(queries[0]),
        )

    def test_le_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number <= 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT " 'SUM("number") "$aggr_number" ' 'FROM "test" ' 'WHERE "number"<=5 ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_in_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number.isin((5, 7))).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'WHERE "number" IN (5,7) '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_notin_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number.notin((5, 7))).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'WHERE "number" NOT IN (5,7) '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_between_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.number.between(5, 7)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'WHERE "number" BETWEEN 5 AND 7 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.number.like("%stuff%")

    def test_not_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.number.not_like("%stuff%")

    def test_is_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.number.is_(True)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class FilterTextFieldTests(TestCase):
    def test_eq_expr_str(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.text == "abc").sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"text\"='abc' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_ne_expr_str(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.text != "abc").sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"text\"<>'abc' "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_gt_expr_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.text > "a"

    def test_ge_expr_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.text >= "a"

    def test_lt_expr_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.text < "a"

    def test_lt_expr_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.text <= "a"

    def test_le_expr_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.text <= "a"

    def test_in_expr_date(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.text.isin(("abc", "def"))).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"text\" IN ('abc','def') "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_notin_expr_date(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.text.notin(("abc", "def"))).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE \"text\" NOT IN ('abc','def') "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_between_expr_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.text.between("a", "b")

    def test_like_expr_str(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.text.like("abc%")).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE LOWER(\"text\") LIKE LOWER('abc%') "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_not_like_expr_str(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.text.not_like("abc%")).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "WHERE NOT LOWER(\"text\") LIKE LOWER('abc%') "
            'ORDER BY 1 '
            "LIMIT 200000",
            str(queries[0]),
        )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class FilterBooleanFieldTests(TestCase):
    def test_eq_expr_bool_true(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.boolean == True).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'WHERE "boolean"=true '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_eq_expr_bool_false(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.boolean == False).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'WHERE "boolean"=false '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_eq_expr_number_1(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.boolean == 1).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT " 'SUM("number") "$aggr_number" ' 'FROM "test" ' 'WHERE "boolean"=1 ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_eq_expr_number_0(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.boolean == 0).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT " 'SUM("number") "$aggr_number" ' 'FROM "test" ' 'WHERE "boolean"=0 ' 'ORDER BY 1 ' 'LIMIT 200000',
            str(queries[0]),
        )

    def test_gt_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean > True

    def test_ge_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean >= True

    def test_lt_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean < True

    def test_le_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean <= True

    def test_between_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean.between(True, False)

    def test_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean.like("%stuff%")

    def test_not_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.boolean.not_like("%stuff%")

    def test_is_true(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.boolean.is_(True)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            'SELECT SUM("number") "$aggr_number" FROM "test" WHERE "boolean" ORDER BY 1 LIMIT 200000',
            str(queries[0]),
        )

    def test_is_false(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.boolean.is_(False)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'WHERE NOT "boolean" '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class FilterAggregateNumberFieldTests(TestCase):
    def test_eq_expr_int(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number == 1).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")=1 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_eq_expr_float(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number == 1.0).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")=1.0 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_ne_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number != 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")<>5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_gt_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number > 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")>5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_ge_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number >= 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")>=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_lt_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number < 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")<5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_le_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number <= 5).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number")<=5 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_in_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number.isin((5, 7))).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number") IN (5,7) '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_notin_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number.notin((5, 7))).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number") NOT IN (5,7) '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_between_expr(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(ds.fields.aggr_number.between(5, 7)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'HAVING SUM("number") BETWEEN 5 AND 7 '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.aggr_number.like("%stuff%")

    def test_not_like_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.aggr_number.not_like("%stuff%")

    def test_is_raises_exception(self):
        with self.assertRaises(DataSetFilterException):
            ds.fields.aggr_number.is_(True)
