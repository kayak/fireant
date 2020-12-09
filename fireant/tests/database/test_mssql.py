import copy
from datetime import datetime
from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

import pytz
from pypika import Field

import fireant as f
from fireant import MSSQLDatabase
from fireant.tests.dataset.mocks import mock_dataset


class TestMSSQLDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mssql = MSSQLDatabase(database='testdb')

    def test_defaults(self):
        self.assertEqual('localhost', self.mssql.host)
        self.assertEqual(1433, self.mssql.port)
        self.assertIsNone(self.mssql.user)
        self.assertIsNone(self.mssql.password)

    def test_connect(self):
        mock_pymssql = Mock()
        with patch.dict('sys.modules', pymssql=mock_pymssql):
            mock_pymssql.connect.return_value = 'OK'

            mssql = MSSQLDatabase(
                database='test_database', host='test_host', port=1234, user='test_user', password='password'
            )
            result = mssql.connect()

        self.assertEqual('OK', result)
        mock_pymssql.connect.assert_called_once_with(
            server='test_host',
            port=1234,
            database='test_database',
            user='test_user',
            password='password',
        )

    def test_convert_date(self):
        result = self.mssql.convert_date(datetime(2020, 1, 2, 5, 0, 0, tzinfo=pytz.UTC))

        self.assertEqual("CAST('2020-01-02T05:00:00+00:00' AS DATETIMEOFFSET)", str(result))

    def test_trunc_hour(self):
        result = self.mssql.trunc_date(Field('date'), 'hour')

        self.assertEqual('DATEADD(hour,DATEDIFF(hour,0,"date"),0)', str(result))

    def test_trunc_day(self):
        result = self.mssql.trunc_date(Field('date'), 'day')

        self.assertEqual('DATEADD(day,DATEDIFF(day,0,"date"),0)', str(result))

    def test_trunc_week(self):
        result = self.mssql.trunc_date(Field('date'), 'week')

        self.assertEqual('DATEADD(week,DATEDIFF(week,0,"date"),0)', str(result))

    def test_trunc_month(self):
        result = self.mssql.trunc_date(Field('date'), 'month')

        self.assertEqual('DATEADD(month,DATEDIFF(month,0,"date"),0)', str(result))

    def test_trunc_quarter(self):
        result = self.mssql.trunc_date(Field('date'), 'quarter')

        self.assertEqual('DATEADD(quarter,DATEDIFF(quarter,0,"date"),0)', str(result))

    def test_trunc_year(self):
        result = self.mssql.trunc_date(Field('date'), 'year')

        self.assertEqual('DATEADD(year,DATEDIFF(year,0,"date"),0)', str(result))

    def test_date_add_hour(self):
        result = self.mssql.date_add(Field('date'), 'hour', 1)

        self.assertEqual('DATEADD(hour,1,"date")', str(result))

    def test_date_add_day(self):
        result = self.mssql.date_add(Field('date'), 'day', 1)

        self.assertEqual('DATEADD(day,1,"date")', str(result))

    def test_date_add_week(self):
        result = self.mssql.date_add(Field('date'), 'week', 1)

        self.assertEqual('DATEADD(week,1,"date")', str(result))

    def test_date_add_month(self):
        result = self.mssql.date_add(Field('date'), 'month', 1)

        self.assertEqual('DATEADD(month,1,"date")', str(result))

    def test_date_add_quarter(self):
        result = self.mssql.date_add(Field('date'), 'quarter', 1)

        self.assertEqual('DATEADD(quarter,1,"date")', str(result))

    def test_date_add_year(self):
        result = self.mssql.date_add(Field('date'), 'year', 1)

        self.assertEqual('DATEADD(year,1,"date")', str(result))

    def test_to_char(self):
        db = MSSQLDatabase()

        to_char = db.to_char(Field('field'))
        self.assertEqual('CAST("field" AS VARCHAR)', str(to_char))

    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    @patch.object(MSSQLDatabase, 'fetch')
    def test_get_column_definitions(self, mock_fetch):
        MSSQLDatabase().get_column_definitions('test_schema', 'test_table')

        mock_fetch.assert_called_once_with(
            'SELECT DISTINCT "COLUMN_NAME","DATA_TYPE" '
            'FROM "INFORMATION_SCHEMA"."COLUMNS" '
            'WHERE "TABLE_SCHEMA"=%(schema)s AND "TABLE_NAME"=%(table)s '
            'ORDER BY "column_name"',
            connection=None,
            parameters={'schema': 'test_schema', 'table': 'test_table'},
        )


class MSSQLSQLBuilderTests(TestCase):
    def test_does_not_group_rollup_column(self):
        mssql_mock_dataset = copy.deepcopy(mock_dataset)
        mssql_mock_dataset.database = MSSQLDatabase()

        queries = (
            mssql_mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.Rollup(mock_dataset.fields.political_party))
            .sql
        )

        self.assertEqual(
            "SELECT "
            "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'ORDER BY "$political_party" '
            'OFFSET 0 ROWS '
            'FETCH NEXT 200000 ROWS ONLY',
            str(queries[1]),
        )
