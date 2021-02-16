from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

from pypika import Field

from fireant.database import PostgreSQLDatabase


class TestPostgreSQL(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = PostgreSQLDatabase()

    def test_defaults(self):
        self.assertEqual('localhost', self.database.host)
        self.assertEqual(5432, self.database.port)
        self.assertIsNone(self.database.database)
        self.assertIsNone(self.database.password)

    def test_connect(self):
        mock_postgresql = Mock()
        with patch.dict('sys.modules', psycopg2=mock_postgresql):
            mock_postgresql.connect.return_value = 'OK'

            postgresql = PostgreSQLDatabase('test_host', 1234, 'test_database', 'test_user', 'password')
            result = postgresql.connect()

        self.assertEqual('OK', result)
        mock_postgresql.connect.assert_called_once_with(
            host='test_host',
            port=1234,
            dbname='test_database',
            user='test_user',
            password='password',
        )

    def test_trunc_hour(self):
        result = self.database.trunc_date(Field('date'), 'hour')

        self.assertEqual('DATE_TRUNC(\'hour\',"date")', str(result))

    def test_trunc_day(self):
        result = self.database.trunc_date(Field('date'), 'day')

        self.assertEqual('DATE_TRUNC(\'day\',"date")', str(result))

    def test_trunc_week(self):
        result = self.database.trunc_date(Field('date'), 'week')

        self.assertEqual('DATE_TRUNC(\'week\',"date")', str(result))

    def test_trunc_quarter(self):
        result = self.database.trunc_date(Field('date'), 'quarter')

        self.assertEqual('DATE_TRUNC(\'quarter\',"date")', str(result))

    def test_trunc_year(self):
        result = self.database.trunc_date(Field('date'), 'year')

        self.assertEqual('DATE_TRUNC(\'year\',"date")', str(result))

    def test_date_add_hour(self):
        result = self.database.date_add(Field('date'), 'hour', 1)

        self.assertEqual('"date" + INTERVAL \'1 HOUR\'', str(result))

    def test_date_add_day(self):
        result = self.database.date_add(Field('date'), 'day', 1)

        self.assertEqual('"date" + INTERVAL \'1 DAY\'', str(result))

    def test_date_add_week(self):
        result = self.database.date_add(Field('date'), 'week', 1)

        self.assertEqual('"date" + INTERVAL \'1 WEEK\'', str(result))

    def test_date_add_month(self):
        result = self.database.date_add(Field('date'), 'month', 1)

        self.assertEqual('"date" + INTERVAL \'1 MONTH\'', str(result))

    def test_date_add_quarter(self):
        result = self.database.date_add(Field('date'), 'quarter', 1)

        self.assertEqual('"date" + INTERVAL \'3 MONTH\'', str(result))

    def test_date_add_year(self):
        result = self.database.date_add(Field('date'), 'year', 1)

        self.assertEqual('"date" + INTERVAL \'1 YEAR\'', str(result))

    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    @patch.object(PostgreSQLDatabase, 'fetch')
    def test_get_column_definitions(self, mock_fetch):
        PostgreSQLDatabase().get_column_definitions('test_schema', 'test_table')

        mock_fetch.assert_called_once_with(
            'SELECT DISTINCT "column_name","data_type" '
            'FROM "information_schema"."columns" '
            'WHERE "table_schema"=%(schema)s AND "table_name"=%(table)s '
            'ORDER BY "column_name"',
            connection=None,
            parameters={'schema': 'test_schema', 'table': 'test_table'},
        )
