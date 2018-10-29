from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

from fireant import (
    annually,
    daily,
    hourly,
    quarterly,
    weekly,
)
from fireant.database import PostgreSQLDatabase
from pypika import Field


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

            postgresql = PostgreSQLDatabase('test_host', 1234, 'test_database',
                                            'test_user', 'password')
            result = postgresql.connect()

        self.assertEqual('OK', result)
        mock_postgresql.connect.assert_called_once_with(
            host='test_host', port=1234, dbname='test_database',
            user='test_user', password='password',
        )

    def test_trunc_hour(self):
        result = self.database.trunc_date(Field('date'), hourly)

        self.assertEqual('DATE_TRUNC(\'hour\',"date")', str(result))

    def test_trunc_day(self):
        result = self.database.trunc_date(Field('date'), daily)

        self.assertEqual('DATE_TRUNC(\'day\',"date")', str(result))

    def test_trunc_week(self):
        result = self.database.trunc_date(Field('date'), weekly)

        self.assertEqual('DATE_TRUNC(\'week\',"date")', str(result))

    def test_trunc_quarter(self):
        result = self.database.trunc_date(Field('date'), quarterly)

        self.assertEqual('DATE_TRUNC(\'quarter\',"date")', str(result))

    def test_trunc_year(self):
        result = self.database.trunc_date(Field('date'), annually)

        self.assertEqual('DATE_TRUNC(\'year\',"date")', str(result))

    def test_date_add_hour(self):
        result = self.database.date_add(Field('date'), 'hour', 1)

        self.assertEqual('DATE_ADD(\'hour\',1,"date")', str(result))

    def test_date_add_day(self):
        result = self.database.date_add(Field('date'), 'day', 1)

        self.assertEqual('DATE_ADD(\'day\',1,"date")', str(result))

    def test_date_add_week(self):
        result = self.database.date_add(Field('date'), 'week', 1)

        self.assertEqual('DATE_ADD(\'week\',1,"date")', str(result))

    def test_date_add_month(self):
        result = self.database.date_add(Field('date'), 'month', 1)

        self.assertEqual('DATE_ADD(\'month\',1,"date")', str(result))

    def test_date_add_quarter(self):
        result = self.database.date_add(Field('date'), 'quarter', 1)

        self.assertEqual('DATE_ADD(\'quarter\',1,"date")', str(result))

    def test_date_add_year(self):
        result = self.database.date_add(Field('date'), 'year', 1)

        self.assertEqual('DATE_ADD(\'year\',1,"date")', str(result))
