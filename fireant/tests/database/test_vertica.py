import logging
from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

from pypika import Column as PypikaColumn, Field, VerticaQuery

from fireant.database import (
    VerticaDatabase,
    VerticaTypeEngine,
)
from fireant.database.sql_types import VarChar


class TestVertica(TestCase):
    def test_defaults(self):
        vertica = VerticaDatabase()

        self.assertEqual('localhost', vertica.host)
        self.assertEqual(5433, vertica.port)
        self.assertEqual('vertica', vertica.database)
        self.assertEqual('vertica', vertica.user)
        self.assertIsNone(vertica.password)
        self.assertIsNone(vertica.connection_timeout)

    def test_connect(self):
        mock_vertica = Mock()
        with patch.dict('sys.modules', vertica_python=mock_vertica):
            mock_vertica.connect.return_value = 'OK'

            vertica = VerticaDatabase('test_host', 1234, 'test_database', 'test_user', 'password', 300, logging.WARNING)
            result = vertica.connect()

        self.assertEqual('OK', result)
        mock_vertica.connect.assert_called_once_with(
            host='test_host',
            port=1234,
            database='test_database',
            user='test_user',
            password='password',
            connection_timeout=300,
            unicode_error='replace',
            log_level=logging.WARNING,
        )

    def test_trunc_hour(self):
        result = VerticaDatabase().trunc_date(Field('date'), 'hour')

        self.assertEqual('TRUNC("date",\'HH\')', str(result))

    def test_trunc_day(self):
        result = VerticaDatabase().trunc_date(Field('date'), 'day')

        self.assertEqual('TRUNC("date",\'DD\')', str(result))

    def test_trunc_week(self):
        result = VerticaDatabase().trunc_date(Field('date'), 'week')

        self.assertEqual('TRUNC("date",\'IW\')', str(result))

    def test_trunc_quarter(self):
        result = VerticaDatabase().trunc_date(Field('date'), 'quarter')

        self.assertEqual('TRUNC("date",\'Q\')', str(result))

    def test_trunc_year(self):
        result = VerticaDatabase().trunc_date(Field('date'), 'year')

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

    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    @patch.object(VerticaDatabase, 'fetch')
    def test_get_column_definitions(self, mock_fetch):
        VerticaDatabase().get_column_definitions('test_schema', 'test_table')

        expected_query = (
            '(SELECT DISTINCT "column_name","data_type" FROM "view_columns" '
            'WHERE "table_schema"=:schema AND "table_name"=:table) '
            'UNION '
            '(SELECT DISTINCT "column_name","data_type" FROM "columns" '
            'WHERE "table_schema"=:schema AND "table_name"=:table)'
        )

        mock_fetch.assert_called_once_with(
            expected_query, connection=None, parameters={'schema': 'test_schema', 'table': 'test_table'}
        )

    @patch.object(VerticaDatabase, 'execute')
    def test_import_csv(self, mock_execute):
        VerticaDatabase().import_csv('abc', '/path/to/file')

        mock_execute.assert_called_once_with(
            'COPY "abc" FROM LOCAL \'/path/to/file\' PARSER fcsvparser(header=false)', connection=None
        )

    @patch.object(VerticaDatabase, 'execute')
    def test_create_temporary_table_from_columns(self, mock_execute):
        columns = PypikaColumn('a', 'varchar'), PypikaColumn('b', 'varchar(100)')

        VerticaDatabase().create_temporary_table_from_columns('abc', columns)

        mock_execute.assert_called_once_with(
            'CREATE LOCAL TEMPORARY TABLE "abc" ("a" varchar,"b" varchar(100)) ON COMMIT PRESERVE ROWS', connection=None
        )

    @patch.object(VerticaDatabase, 'execute')
    def test_create_temporary_table_from_select(self, mock_execute):
        query = VerticaQuery.from_('abc').select('*')

        VerticaDatabase().create_temporary_table_from_select('def', query)

        mock_execute.assert_called_once_with(
            'CREATE LOCAL TEMPORARY TABLE "def" ON COMMIT PRESERVE ROWS AS (SELECT * FROM "abc")', connection=None
        )


class TestVerticaTypeEngine(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.vertica_type_engine = VerticaTypeEngine()

    def test_to_ansi(self):
        db_type = 'varchar2'

        ansi_type = self.vertica_type_engine.to_ansi(db_type)

        self.assertTrue(isinstance(ansi_type, VarChar))

    def test_from_ansi(self):
        ansi_type = VarChar()

        db_type = self.vertica_type_engine.from_ansi(ansi_type)

        self.assertEqual('varchar', db_type)
