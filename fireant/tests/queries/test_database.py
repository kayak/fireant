from unittest import TestCase
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

import pandas as pd
import time

from fireant.queries.execution import fetch_as_dataframe
from fireant.tests.dataset.mocks import (
    dimx1_str_df,
    dimx2_date_str_df,
)
from fireant.utils import alias_selector as fd


class FetchDataTests(TestCase):
    def setUp(self):
        self.mock_database = Mock()
        self.mock_database.slow_query_log_min_seconds = 15
        self.mock_database.cache_middleware = None

        mock_connect = self.mock_database.connect.return_value = MagicMock()
        self.mock_connection = mock_connect.__enter__.return_value
        mock_cursor_func = self.mock_connection.cursor
        mock_cursor = mock_cursor_func.return_value = MagicMock(name='mock_cursor')
        self.mock_data_frame = mock_cursor.fetchall.return_value = MagicMock(name='data_frame')

        self.mock_query = 'SELECT *'
        self.mock_dimensions = [Mock(), Mock()]
        self.mock_dimensions[0].is_rollup = False
        self.mock_dimensions[1].is_rollup = True

    def test_do_fetch_data_calls_database_fetch_data(self, ):
        with patch('fireant.queries.execution.pd.read_sql', return_value=self.mock_data_frame) as mock_read_sql:
            fetch_as_dataframe(self.mock_query, self.mock_database)

            mock_read_sql.assert_called_once_with(self.mock_query,
                                                  self.mock_connection,
                                                  coerce_float=True,
                                                  parse_dates=True)


@patch('fireant.queries.execution.pd.read_sql')
class FetchDataLoggingTests(TestCase):
    def setUp(self):
        self.mock_query = 'SELECT *'

        self.mock_database = Mock()
        self.mock_database.cache_middleware = None
        self.mock_database.slow_query_log_min_seconds = 15

        mock_connect = self.mock_database.connect.return_value = MagicMock()
        mock_cursor_func = mock_connect.__enter__.return_value.cursor
        mock_cursor = mock_cursor_func.return_value = MagicMock(name='mock_cursor')
        mock_cursor.fetchall.return_value = 'OK'

        self.mock_dimensions = [Mock(), Mock()]
        self.mock_dimensions[0].is_rollup = False
        self.mock_dimensions[1].is_rollup = False

    @patch('fireant.queries.execution.query_logger')
    def test_debug_query_log_called_with_query(self, mock_logger, *mocks):
        fetch_as_dataframe(self.mock_query, self.mock_database)

        mock_logger.debug.assert_called_once_with('SELECT *')

    @patch.object(time, 'time', return_value=1520520255.0)
    @patch('fireant.queries.execution.query_logger')
    def test_info_query_log_called_with_query_and_duration(self, mock_logger, *mocks):
        fetch_as_dataframe(self.mock_query, self.mock_database)

        mock_logger.info.assert_called_once_with('[0.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.queries.execution.slow_query_logger')
    def test_warning_slow_query_logger_called_with_duration_and_query_if_over_slow_query_limit(self,
                                                                                               mock_logger,
                                                                                               mock_time,
                                                                                               *mocks):
        mock_time.side_effect = [1520520255.0, 1520520277.0]
        fetch_as_dataframe(self.mock_query, self.mock_database)

        mock_logger.warning.assert_called_once_with('[22.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.queries.execution.slow_query_logger')
    def test_warning_slow_query_logger_not_called_with_duration_and_query_if_not_over_slow_query_limit(self,
                                                                                                       mock_logger,
                                                                                                       mock_time,
                                                                                                       *mocks):
        mock_time.side_effect = [1520520763.0, 1520520764.0]
        fetch_as_dataframe(self.mock_query, self.mock_database)

        mock_logger.warning.assert_not_called()


dimx1_str_nans_df = dimx1_str_df.append(
      pd.DataFrame([[300, 2]],
                   columns=dimx1_str_df.columns,
                   index=pd.Index([None],
                                  name=dimx1_str_df.index.name)))


def add_nans(df):
    return pd.DataFrame([[300, 2]],
                        columns=df.columns,
                        index=pd.Index([None], name=df.index.names[1]))


dimx2_date_str_nans_df = dimx2_date_str_df \
    .append(dimx2_date_str_df.groupby(level=fd('timestamp')).apply(add_nans)) \
    .sort_index()


def totals(df):
    return pd.DataFrame([list(df.sum())],
                        columns=df.columns,
                        index=pd.Index([None], name=df.index.names[1]))


dimx2_date_str_nans_totals_df = dimx2_date_str_nans_df \
    .append(dimx2_date_str_nans_df.groupby(level=fd('timestamp')).apply(totals)) \
    .sort_index() \
    .sort_index(level=[0, 1], ascending=False)  # This sorts the DF so that the first instance of NaN is the totals


class FetchDimensionOptionsTests(TestCase):
    pass  # TODO
