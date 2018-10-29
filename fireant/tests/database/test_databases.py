from unittest import TestCase
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

import time

from fireant.database import Database
from fireant.database.base import fetch_data
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
            db.trunc_date(Field('abc'), 'day')

    def test_to_char(self):
        db = Database()

        to_char = db.to_char(Field('field'))
        self.assertEqual(str(to_char), 'CAST("field" AS VARCHAR)')


class FetchDataLoggingTests(TestCase):
    def setUp(self):
        self.mock_database = Mock(name='database')
        self.mock_database.slow_query_log_min_seconds = 15
        self.mock_database.connect.return_value = MagicMock()
        self.mock_query = 'SELECT *'

    @patch('fireant.database.base.query_logger')
    def test_debug_query_log_called_with_query(self, mock_logger):
        fetch_data(self.mock_database, self.mock_query)

        mock_logger.debug.assert_called_once_with('SELECT *')

    @patch.object(time, 'time', return_value=1520520255.0)
    @patch('fireant.database.base.query_logger')
    def test_info_query_log_called_with_query_and_duration(self, mock_logger, *mocks):
        fetch_data(self.mock_database, self.mock_query)

        mock_logger.info.assert_called_once_with('[0.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.database.base.slow_query_logger')
    def test_warning_slow_query_logger_called_with_duration_and_query_if_over_slow_query_limit(self,
                                                                                               mock_logger,
                                                                                               mock_time):
        mock_time.side_effect = [1520520255.0, 1520520277.0]
        fetch_data(self.mock_database, self.mock_query)

        mock_logger.warning.assert_called_once_with('[22.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.database.base.slow_query_logger')
    def test_warning_slow_query_logger_not_called_with_duration_and_query_if_not_over_slow_query_limit(self,
                                                                                                       mock_logger,
                                                                                                       mock_time):
        mock_time.side_effect = [1520520763.0, 1520520764.0]
        fetch_data(self.mock_database, self.mock_query)

        mock_logger.warning.assert_not_called()
