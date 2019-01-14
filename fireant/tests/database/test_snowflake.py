from unittest import TestCase
from unittest.mock import (
    ANY,
    patch,
)

from fireant.database import SnowflakeDatabase
from fireant.slicer import *
from pypika import Field


class TestSnowflake(TestCase):
    def test_defaults(self):
        snowflake = SnowflakeDatabase()

        self.assertEqual('snowflake', snowflake.account)
        self.assertEqual('snowflake', snowflake.database)
        self.assertEqual('snowflake', snowflake.user)
        self.assertIsNone(snowflake.private_key)
        self.assertIsNone(snowflake.password)
        self.assertIsNone(snowflake.region)
        self.assertIsNone(snowflake.warehouse)

    @patch('fireant.database.snowflake.snowflake')
    def test_connect_with_password(self, mock_snowflake):
        mock_snowflake.connect.return_value = 'OK'

        snowflake = SnowflakeDatabase(user='test_user',
                                      password='test_pass',
                                      account='test_account',
                                      database='test_database')
        result = snowflake.connect()

        self.assertEqual('OK', result)
        mock_snowflake.connect.assert_called_once_with(user='test_user',
                                                       password='test_pass',
                                                       account='test_account',
                                                       database='test_database',
                                                       private_key=None,
                                                       region=None,
                                                       warehouse=None)

    @patch('fireant.database.snowflake.serialization')
    @patch('fireant.database.snowflake.snowflake')
    def test_connect_with_pkey(self, mock_snowflake, mock_serialization):
        mock_pkey = mock_serialization.load_pem_private_key.return_value
        mock_pkey.private_bytes.return_value = 'MY KEY'
        mock_snowflake.connect.return_value = 'OK'

        snowflake = SnowflakeDatabase(user='test_user',
                                      private_key='abcdefg',
                                      pass_phrase='1234',
                                      account='test_account',
                                      database='test_database')
        result = snowflake.connect()

        with self.subTest('returns connection'):
            self.assertEqual('OK', result)

        with self.subTest('connects with credentials'):
            mock_serialization.load_pem_private_key.assert_called_once_with(b'abcdefg',
                                                                            b'1234',
                                                                            backend=ANY)

        with self.subTest('key with private bytes'):
            mock_pkey.private_bytes.assert_called_once()

        with self.subTest('connects with credentials'):
            mock_snowflake.connect.assert_called_once_with(user='test_user',
                                                           password=None,
                                                           account='test_account',
                                                           database='test_database',
                                                           private_key='MY KEY',
                                                           region=None,
                                                           warehouse=None)

    def test_trunc_hour(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), hourly)

        self.assertEqual('TRUNC("date",\'HH\')', str(result))

    def test_trunc_day(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), daily)

        self.assertEqual('TRUNC("date",\'DD\')', str(result))

    def test_trunc_week(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), weekly)

        self.assertEqual('TRUNC("date",\'IW\')', str(result))

    def test_trunc_quarter(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), quarterly)

        self.assertEqual('TRUNC("date",\'Q\')', str(result))

    def test_trunc_year(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), annually)

        self.assertEqual('TRUNC("date",\'Y\')', str(result))

    def test_date_add_hour(self):
        result = SnowflakeDatabase().date_add(Field('date'), 'hour', 1)

        self.assertEqual('TIMESTAMPADD(\'hour\',1,"date")', str(result))

    def test_date_add_day(self):
        result = SnowflakeDatabase().date_add(Field('date'), 'day', 1)

        self.assertEqual('TIMESTAMPADD(\'day\',1,"date")', str(result))

    def test_date_add_week(self):
        result = SnowflakeDatabase().date_add(Field('date'), 'week', 1)

        self.assertEqual('TIMESTAMPADD(\'week\',1,"date")', str(result))

    def test_date_add_month(self):
        result = SnowflakeDatabase().date_add(Field('date'), 'month', 1)

        self.assertEqual('TIMESTAMPADD(\'month\',1,"date")', str(result))

    def test_date_add_quarter(self):
        result = SnowflakeDatabase().date_add(Field('date'), 'quarter', 1)

        self.assertEqual('TIMESTAMPADD(\'quarter\',1,"date")', str(result))

    def test_date_add_year(self):
        result = SnowflakeDatabase().date_add(Field('date'), 'year', 1)

        self.assertEqual('TIMESTAMPADD(\'year\',1,"date")', str(result))
