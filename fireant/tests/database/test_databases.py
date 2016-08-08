# coding: utf-8
from unittest import TestCase

from mock import patch, MagicMock

from fireant.database import Database


class DatabaseTests(TestCase):
    @patch('fireant.database.Database.connect', name='mock_connect')
    def test_fetch(self, mock_connect):
        mock_cursor_func = mock_connect.return_value.__enter__.return_value.cursor
        mock_cursor = mock_cursor_func.return_value = MagicMock(name='mock_cursor')
        mock_cursor.fetchall.return_value = 'OK'

        result = Database().fetch('SELECT 1')

        self.assertEqual('OK', result)
        mock_cursor_func.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with('SELECT 1')
        mock_cursor.fetchall.assert_called_once_with()
