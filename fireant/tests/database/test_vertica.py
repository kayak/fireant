from unittest import TestCase

from unittest.mock import patch, Mock

from fireant import (
    hourly,
    daily,
    weekly,
    quarterly,
    annually,
)
from fireant.database import VerticaDatabase
from pypika import Field


class TestVertica(TestCase):

    def test_defaults(self):
        vertica = VerticaDatabase()

        self.assertEqual('localhost', vertica.host)
        self.assertEqual(5433, vertica.port)
        self.assertEqual('vertica', vertica.database)
        self.assertEqual('vertica', vertica.user)
        self.assertIsNone(vertica.password)
        self.assertIsNone(vertica.read_timeout)

    def test_connect(self):
        mock_vertica = Mock()
        with patch.dict('sys.modules', vertica_python=mock_vertica):
            mock_vertica.connect.return_value = 'OK'

            vertica = VerticaDatabase('test_host', 1234, 'test_database',
                              'test_user', 'password')
            result = vertica.connect()

        self.assertEqual('OK', result)
        mock_vertica.connect.assert_called_once_with(
            host='test_host', port=1234, database='test_database',
            user='test_user', password='password', read_timeout=None,
        )

    def test_trunc_hour(self):
        result = VerticaDatabase().trunc_date(Field('date'), hourly)

        self.assertEqual('TRUNC("date",\'HH\')', str(result))

    def test_trunc_day(self):
        result = VerticaDatabase().trunc_date(Field('date'), daily)

        self.assertEqual('TRUNC("date",\'DD\')', str(result))

    def test_trunc_week(self):
        result = VerticaDatabase().trunc_date(Field('date'), weekly)

        self.assertEqual('TRUNC("date",\'IW\')', str(result))

    def test_trunc_quarter(self):
        result = VerticaDatabase().trunc_date(Field('date'), quarterly)

        self.assertEqual('TRUNC("date",\'Q\')', str(result))

    def test_trunc_year(self):
        result = VerticaDatabase().trunc_date(Field('date'), annually)

        self.assertEqual('TRUNC("date",\'Y\')', str(result))

    def test_date_add_hour(self):
        result = VerticaDatabase().date_add(Field('date'), 'hour', 1)

        self.assertEqual('TIMESTAMPADD(\'hour\',1,"date")', str(result))

    def test_date_add_day(self):
        result = VerticaDatabase().date_add(Field('date'), 'day', 1)

        self.assertEqual('TIMESTAMPADD(\'day\',1,"date")', str(result))

    def test_date_add_week(self):
        result = VerticaDatabase().date_add(Field('date'), 'week', 1)

        self.assertEqual('TIMESTAMPADD(\'week\',1,"date")', str(result))

    def test_date_add_month(self):
        result = VerticaDatabase().date_add(Field('date'), 'month', 1)

        self.assertEqual('TIMESTAMPADD(\'month\',1,"date")', str(result))

    def test_date_add_quarter(self):
        result = VerticaDatabase().date_add(Field('date'), 'quarter', 1)

        self.assertEqual('TIMESTAMPADD(\'quarter\',1,"date")', str(result))

    def test_date_add_year(self):
        result = VerticaDatabase().date_add(Field('date'), 'year', 1)

        self.assertEqual('TIMESTAMPADD(\'year\',1,"date")', str(result))

