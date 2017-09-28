# coding: utf-8
from unittest import TestCase

from mock import (
    Mock,
    patch,
)
from pypika import Field

from fireant.database import PostgreSQLDatabase


class TestPostgreSQL(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
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

            postgresql = PostgreSQLDatabase('test_database', 'test_host', 1234,
                                            'test_user', 'password')
            result = postgresql.connect()

        self.assertEqual('OK', result)
        mock_postgresql.connect.assert_called_once_with(
            host='test_host', port=1234, dbname='test_database',
            user='test_user', password='password',
        )

    def test_trunc_hour(self):
        result = self.database.trunc_date(Field('date'), 'hour')

        self.assertEqual('date_trunc(\'hour\',"date")', str(result))

    def test_trunc_day(self):
        result = self.database.trunc_date(Field('date'), 'day')

        self.assertEqual('date_trunc(\'day\',"date")', str(result))

    def test_trunc_week(self):
        result = self.database.trunc_date(Field('date'), 'week')

        self.assertEqual('date_trunc(\'week\',"date")', str(result))

    def test_trunc_quarter(self):
        result = self.database.trunc_date(Field('date'), 'quarter')

        self.assertEqual('date_trunc(\'quarter\',"date")', str(result))

    def test_trunc_year(self):
        result = self.database.trunc_date(Field('date'), 'year')

        self.assertEqual('date_trunc(\'year\',"date")', str(result))

    def test_date_add_hour(self):
        result = self.database.date_add('hour', 1, Field('date'))

        self.assertEqual('DATE_ADD(\'hour\',1,"date")', str(result))

    def test_date_add_day(self):
        result = self.database.date_add('day', 1, Field('date'))

        self.assertEqual('DATE_ADD(\'day\',1,"date")', str(result))

    def test_date_add_week(self):
        result = self.database.date_add('week', 1, Field('date'))

        self.assertEqual('DATE_ADD(\'week\',1,"date")', str(result))

    def test_date_add_month(self):
        result = self.database.date_add('month', 1, Field('date'))

        self.assertEqual('DATE_ADD(\'month\',1,"date")', str(result))

    def test_date_add_quarter(self):
        result = self.database.date_add('quarter', 1, Field('date'))

        self.assertEqual('DATE_ADD(\'quarter\',1,"date")', str(result))

    def test_date_add_year(self):
        result = self.database.date_add('year', 1, Field('date'))

        self.assertEqual('DATE_ADD(\'year\',1,"date")', str(result))
