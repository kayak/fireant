from unittest import TestCase

from unittest.mock import patch, MagicMock

from fireant.database import Database
from pypika import Field


class DatabaseTests(TestCase):
    @patch('fireant.database.Database.connect', name='mock_connect')
    def test_fetch(self, mock_connect):
        mock_cursor_func = mock_connect.return_value.__enter__.return_value.cursor
        mock_cursor = mock_cursor_func.return_value = MagicMock(name='mock_cursor')
        mock_cursor.fetchall.return_value = 'OK'

        result = Database().fetch('SELECT 1')

        self.assertEqual(mock_cursor.fetchall.return_value, result)
        mock_cursor_func.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with('SELECT 1')
        mock_cursor.fetchall.assert_called_once_with()

    @patch('pandas.read_sql', name='mock_read_sql')
    @patch('fireant.database.Database.connect', name='mock_connect')
    def test_fetch_dataframe(self, mock_connect, mock_read_sql):
        query = 'SELECT 1'
        mock_read_sql.return_value = 'OK'

        result = Database().fetch_data(query)

        self.assertEqual(mock_read_sql.return_value, result)

        mock_read_sql.assert_called_once_with(query, mock_connect().__enter__(), coerce_float=True, parse_dates=True)

    def test_database_api(self):
        db = Database()

        with self.assertRaises(NotImplementedError):
            db.connect()

        with self.assertRaises(NotImplementedError):
            db.trunc_date(Field('abc'), 'DAY')
