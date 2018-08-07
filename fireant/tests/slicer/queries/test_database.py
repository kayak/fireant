import time
from unittest import TestCase
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

import numpy as np
import pandas as pd

from fireant.slicer.queries.database import (
    clean_and_apply_index,
    fetch_data,
)
from fireant.tests.slicer.mocks import (
    cat_dim_df,
    cont_dim_df,
    cont_uni_dim_df,
    single_metric_df,
    slicer,
    uni_dim_df,
)
from fireant.utils import format_dimension_key as fd


class FetchDataTests(TestCase):

    def setUp(self):
        self.mock_database = Mock(name='database')
        self.mock_database.slow_query_log_min_seconds = 15
        self.mock_data_frame = self.mock_database.fetch_data.return_value = MagicMock(name='data_frame')
        self.mock_query = 'SELECT *'
        self.mock_dimensions = [Mock(), Mock()]
        self.mock_dimensions[0].is_rollup = False
        self.mock_dimensions[1].is_rollup = True

    def test_fetch_data_called_on_database(self):
        fetch_data(self.mock_database, self.mock_query, self.mock_dimensions)

        self.mock_database.fetch_data.assert_called_once_with(self.mock_query)

    def test_index_set_on_data_frame_result(self):
        fetch_data(self.mock_database, self.mock_query, self.mock_dimensions)

        self.mock_data_frame.set_index.assert_called_once_with([fd(d.key)
                                                                for d in self.mock_dimensions])

    @patch('fireant.slicer.queries.database.query_logger.debug')
    def test_debug_query_log_called_with_query(self, mock_logger):
        fetch_data(self.mock_database, self.mock_query, self.mock_dimensions)

        mock_logger.assert_called_once_with('SELECT *')

    @patch.object(time, 'time', return_value=1520520255.0)
    @patch('fireant.slicer.queries.database.query_logger.info')
    def test_info_query_log_called_with_query_and_duration(self, mock_logger, mock_time):
        fetch_data(self.mock_database, self.mock_query, self.mock_dimensions)

        mock_logger.assert_called_once_with('[0.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.slicer.queries.database.slow_query_logger.warning')
    def test_warning_slow_query_logger_called_with_duration_and_query_if_over_slow_query_limit(self,
                                                                                               mock_logger,
                                                                                               mock_time):
        mock_time.side_effect = [1520520255.0, 1520520277.0]
        fetch_data(self.mock_database, self.mock_query, self.mock_dimensions)

        mock_logger.assert_called_once_with('[22.0 seconds]: SELECT *')

    @patch.object(time, 'time')
    @patch('fireant.slicer.queries.database.slow_query_logger.warning')
    def test_warning_slow_query_logger_not_called_with_duration_and_query_if_not_over_slow_query_limit(self,
                                                                                                       mock_logger,
                                                                                                       mock_time):
        mock_time.side_effect = [1520520763.0, 1520520764.0]
        fetch_data(self.mock_database, self.mock_query, self.mock_dimensions)

        mock_logger.assert_not_called()


cat_dim_nans_df = cat_dim_df.append(
    pd.DataFrame([[300, 2]],
                 columns=cat_dim_df.columns,
                 index=pd.Index([None],
                                name=cat_dim_df.index.name)))
uni_dim_nans_df = uni_dim_df.append(
    pd.DataFrame([[None, 300, 2]],
                 columns=uni_dim_df.columns,
                 index=pd.Index([None],
                                name=uni_dim_df.index.name)))


def add_nans(df):
    return pd.DataFrame([[None, 300, 2]],
                        columns=df.columns,
                        index=pd.Index([None], name=df.index.names[1]))


cont_uni_dim_nans_df = cont_uni_dim_df \
    .append(cont_uni_dim_df.groupby(level=fd('timestamp')).apply(add_nans)) \
    .sort_index()


def totals(df):
    return pd.DataFrame([[None] + list(df.sum())],
                        columns=df.columns,
                        index=pd.Index([None], name=df.index.names[1]))


cont_uni_dim_nans_totals_df = cont_uni_dim_nans_df \
    .append(cont_uni_dim_nans_df.groupby(level=fd('timestamp')).apply(totals)) \
    .sort_index() \
    .sort_index(level=[0, 1], ascending=False)  # This sorts the DF so that the first instance of NaN is the totals


class FetchDataCleanIndexTests(TestCase):
    maxDiff = None

    def test_do_nothing_when_no_dimensions(self):
        result = clean_and_apply_index(single_metric_df, [])

        pd.testing.assert_frame_equal(result, single_metric_df)

    def test_set_time_series_index_level(self):
        result = clean_and_apply_index(cont_dim_df.reset_index(),
                                       [slicer.dimensions.timestamp])

        self.assertListEqual(result.index.names, cont_dim_df.index.names)

    def test_set_cat_dim_index(self):
        result = clean_and_apply_index(cat_dim_df.reset_index(),
                                       [slicer.dimensions.political_party])

        self.assertListEqual(result.index.tolist(), ['d', 'i', 'r'])

    def test_set_cat_dim_index_with_nan_converted_to_empty_str(self):
        result = clean_and_apply_index(cat_dim_nans_df.reset_index(),
                                       [slicer.dimensions.political_party])

        self.assertListEqual(result.index.tolist(), ['d', 'i', 'r', 'null'])

    def test_convert_cat_totals_converted_to_totals(self):
        result = clean_and_apply_index(cat_dim_nans_df.reset_index(),
                                       [slicer.dimensions.political_party.rollup()])

        self.assertListEqual(result.index.tolist(), ['d', 'i', 'r', 'totals'])

    def test_convert_numeric_values_to_string(self):
        result = clean_and_apply_index(uni_dim_df.reset_index(), [slicer.dimensions.candidate])
        self.assertEqual(result.index.dtype.type, np.object_)

    def test_set_uni_dim_index(self):
        result = clean_and_apply_index(uni_dim_df.reset_index(),
                                       [slicer.dimensions.candidate])

        self.assertListEqual(result.index.tolist(), [str(x + 1) for x in range(11)])

    def test_set_uni_dim_index_with_nan_converted_to_empty_str(self):
        result = clean_and_apply_index(uni_dim_nans_df.reset_index(),
                                       [slicer.dimensions.candidate])

        self.assertListEqual(result.index.tolist(), [str(x + 1) for x in range(11)] + ['null'])

    def test_convert_uni_totals(self):
        result = clean_and_apply_index(uni_dim_nans_df.reset_index(),
                                       [slicer.dimensions.candidate.rollup()])

        self.assertListEqual(result.index.tolist(), [str(x + 1) for x in range(11)] + ['totals'])

    def test_set_index_for_multiindex_with_nans_and_totals(self):
        result = clean_and_apply_index(cont_uni_dim_nans_totals_df.reset_index(),
                                       [slicer.dimensions.timestamp, slicer.dimensions.state.rollup()])

        self.assertListEqual(result.index.get_level_values(1).unique().tolist(), ['2', '1', 'null', 'totals'])


class FetchDimensionOptionsTests(TestCase):
    pass  # TODO
