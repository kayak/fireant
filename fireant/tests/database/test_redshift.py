from unittest.mock import (
    Mock,
    patch,
)

from fireant.database import RedshiftDatabase
from .test_postgresql import TestPostgreSQL


class TestRedshift(TestPostgreSQL):
    """ Inherits from TestPostgreSQL as Redshift is almost identical to PostgreSQL so the tests are similar """

    @classmethod
    def setUpClass(cls):
        cls.database = RedshiftDatabase()

    def test_defaults(self):
        self.assertEqual('localhost', self.database.host)
        self.assertEqual(5439, self.database.port)
        self.assertIsNone(self.database.database)
        self.assertIsNone(self.database.password)

    def test_connect(self):
        mock_redshift = Mock()
        with patch.dict('sys.modules', psycopg2=mock_redshift):
            mock_redshift.connect.return_value = 'OK'

            redshift = RedshiftDatabase('test_host', 1234, 'test_database', 'test_user', 'password')
            result = redshift.connect()

        self.assertEqual('OK', result)
        mock_redshift.connect.assert_called_once_with(
            host='test_host',
            port=1234,
            dbname='test_database',
            user='test_user',
            password='password',
        )
