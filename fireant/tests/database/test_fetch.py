import time
from unittest.case import TestCase
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

from fireant import Database
from fireant.middleware import log_middleware


class FetchDataTests(TestCase):
    def setUp(self):
        self.database = Database(middlewares=[])
        self.database.slow_query_log_min_seconds = 15

        mock_connect = self.database.connect = MagicMock()
        self.mock_connection = mock_connect.return_value.__enter__.return_value
        mock_cursor_func = self.mock_connection.cursor
        mock_cursor = mock_cursor_func.return_value = MagicMock(name='mock_cursor')
        self.mock_data_frame = mock_cursor.fetchall.return_value = MagicMock(name='data_frame')

        self.mock_query = 'SELECT *'
        self.mock_dimensions = [Mock(), Mock()]
        self.mock_dimensions[0].is_rollup = False
        self.mock_dimensions[1].is_rollup = True

    def test_do_fetch_data_calls_database_fetch_data(self):
        with patch('fireant.queries.execution.pd.read_sql', return_value=self.mock_data_frame) as mock_read_sql:
            self.database.fetch_dataframe(self.mock_query, parse_dates="hi")

            mock_read_sql.assert_called_once_with(
                self.mock_query, self.mock_connection, coerce_float=True, parse_dates="hi"
            )


@patch('fireant.queries.execution.pd.read_sql')
class FetchDataLoggingTests(TestCase):
    def setUp(self):
        self.mock_query = 'SELECT *'

        self.database = Database(middlewares=[log_middleware])
        self.database.slow_query_log_min_seconds = 15

        mock_connect = self.database.connect = MagicMock()
        mock_cursor_func = mock_connect.__enter__.return_value.cursor
        mock_cursor = mock_cursor_func.return_value = MagicMock(name='mock_cursor')
        mock_cursor.fetchall.return_value = 'OK'

        self.mock_dimensions = [Mock(), Mock()]
        self.mock_dimensions[0].is_rollup = False
        self.mock_dimensions[1].is_rollup = False

    @patch('fireant.middleware.decorators.query_logger')
    def test_debug_query_log_called_with_query(self, mock_logger, *mocks):
        self.database.fetch_dataframe(self.mock_query)

        mock_logger.debug.assert_called_once_with('SELECT *')

    @patch.object(time, 'time', return_value=1520520255.0)
    @patch('fireant.middleware.decorators.query_logger')
    def test_info_query_log_called_with_query_and_duration(self, mock_logger, *mocks):
        self.database.fetch_dataframe(self.mock_query)

        mock_logger.info.assert_called_once_with('[0.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.middleware.decorators.slow_query_logger')
    def test_warning_slow_query_logger_called_with_duration_and_query_if_over_slow_query_limit(
        self, mock_logger, mock_time, *mocks
    ):
        mock_time.side_effect = [1520520255.0, 1520520277.0]
        self.database.fetch_dataframe(self.mock_query)

        mock_logger.warning.assert_called_once_with('[22.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.middleware.decorators.slow_query_logger')
    def test_warning_slow_query_logger_not_called_with_duration_and_query_if_not_over_slow_query_limit(
        self, mock_logger, mock_time, *mocks
    ):
        mock_time.side_effect = [1520520763.0, 1520520764.0]
        self.database.fetch_dataframe(self.mock_query)

        mock_logger.warning.assert_not_called()
