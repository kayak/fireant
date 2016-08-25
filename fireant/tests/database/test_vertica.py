# coding: utf-8
from unittest import TestCase

from mock import patch, Mock

from fireant.database.vertica import Vertica
from pypika import Field


class TestVertica(TestCase):
    def test_defaults(self):
        vertica = Vertica()

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

            vertica = Vertica('test_host', 1234, 'test_database',
                              'test_user', 'password')
            result = vertica.connect()

        self.assertEqual('OK', result)
        mock_vertica.connect.assert_called_once_with(
            host='test_host', port=1234, database='test_database',
            user='test_user', password='password', read_timeout=None,
        )

    def test_round_date(self):
        result = Vertica().round_date(Field('date'), 'XX')

        self.assertEqual('ROUND("date",\'XX\')', str(result))
