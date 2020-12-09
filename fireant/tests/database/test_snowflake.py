from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from pypika import Field

from fireant.database import SnowflakeDatabase


class TestSnowflake(TestCase):
    def test_defaults(self):
        snowflake = SnowflakeDatabase()

        self.assertEqual('snowflake', snowflake.account)
        self.assertEqual('snowflake', snowflake.database)
        self.assertEqual('snowflake', snowflake.user)
        self.assertIsNone(snowflake.password)
        self.assertIsNone(snowflake.private_key_data)
        self.assertIsNone(snowflake.private_key_password)
        self.assertIsNone(snowflake.region)
        self.assertIsNone(snowflake.warehouse)

    def test_connect_with_password(self):
        mock_snowflake = Mock(name='mock_snowflake')
        mock_connector = mock_snowflake.connector

        # need to patch this here so it can be imported in the function scope
        with patch.dict('sys.modules', snowflake=mock_snowflake):
            mock_connector.connect.return_value = 'OK'

            snowflake = SnowflakeDatabase(
                user='test_user', password='test_pass', account='test_account', database='test_database'
            )
            result = snowflake.connect()

        self.assertEqual('OK', result)
        mock_connector.connect.assert_called_once_with(
            user='test_user',
            password='test_pass',
            account='test_account',
            database='test_database',
            private_key=None,
            region=None,
            warehouse=None,
        )

    @patch('fireant.database.snowflake.serialization')
    def test_connect_with_pkey(self, mock_serialization):
        mock_snowflake = Mock(name='mock_snowflake')
        mock_connector = mock_snowflake.connector
        mock_pkey = mock_serialization.load_pem_private_key.return_value = Mock(name='pkey')

        # need to patch this here so it can be imported in the function scope
        with patch.dict('sys.modules', snowflake=mock_snowflake):
            mock_connector.connect.return_value = 'OK'

            snowflake = SnowflakeDatabase(
                user='test_user',
                private_key_data='abcdefg',
                private_key_password='1234',
                account='test_account',
                database='test_database',
            )
            result = snowflake.connect()

        with self.subTest('returns connection'):
            self.assertEqual('OK', result)

        with self.subTest('connects with credentials'):
            mock_serialization.load_pem_private_key.assert_called_once_with(b'abcdefg', b'1234', backend=ANY)

        with self.subTest('connects with credentials'):
            mock_connector.connect.assert_called_once_with(
                user='test_user',
                password=None,
                account='test_account',
                database='test_database',
                private_key=mock_pkey.private_bytes.return_value,
                region=None,
                warehouse=None,
            )

    def test_trunc_hour(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), 'hour')

        self.assertEqual('TRUNC("date",\'HH\')', str(result))

    def test_trunc_day(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), 'day')

        self.assertEqual('TRUNC("date",\'DD\')', str(result))

    def test_trunc_week(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), 'week')

        self.assertEqual('TRUNC("date",\'IW\')', str(result))

    def test_trunc_quarter(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), 'quarter')

        self.assertEqual('TRUNC("date",\'Q\')', str(result))

    def test_trunc_year(self):
        result = SnowflakeDatabase().trunc_date(Field('date'), 'year')

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

    @patch.object(SnowflakeDatabase, 'fetch')
    def test_get_column_definitions(self, mock_fetch):
        SnowflakeDatabase().get_column_definitions('test_schema', 'test_table')

        mock_fetch.assert_called_once_with(
            'SELECT DISTINCT COLUMN_NAME,DATA_TYPE '
            'FROM INFORMATION_SCHEMA.COLUMNS '
            'WHERE TABLE_SCHEMA=%(schema)s AND TABLE_NAME=%(table)s '
            'ORDER BY column_name',
            connection=None,
            parameters={'schema': 'test_schema', 'table': 'test_table'},
        )
