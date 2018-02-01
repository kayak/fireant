from unittest import TestCase
from unittest.mock import Mock

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


class FetchDataTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_database = Mock(name='database')
        cls.mock_data_frame = cls.mock_database.fetch_data.return_value = Mock(name='data_frame')
        cls.mock_query = 'SELECT *'
        cls.mock_dimensions = [Mock(), Mock()]

        cls.result = fetch_data(cls.mock_database, cls.mock_query, cls.mock_dimensions)

    def test_fetch_data_called_on_database(self):
        self.mock_database.fetch_data.assert_called_once_with(self.mock_query)

    def test_index_set_on_data_frame_result(self):
        self.mock_data_frame.set_index.assert_called_once_with([d.key for d in self.mock_dimensions])


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
    .append(cont_uni_dim_df.groupby(level='timestamp').apply(add_nans)) \
    .sort_index()


def totals(df):
    return pd.DataFrame([[None] + list(df.sum())],
                        columns=df.columns,
                        index=pd.Index([None], name=df.index.names[1]))


cont_uni_dim_nans_totals_df = cont_uni_dim_nans_df \
    .append(cont_uni_dim_nans_df.groupby(level='timestamp').apply(totals))\
    .sort_index() \
    .sort_index(level=[0, 1], ascending=False)  # This sorts the DF so that the first instance of NaN is the totals


class FetchDataCleanIndexTests(TestCase):
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

        self.assertListEqual(list(result.index), ['d', 'i', 'r'])

    def test_set_cat_dim_index_with_nan_converted_to_empty_str(self):
        result = clean_and_apply_index(cat_dim_nans_df.reset_index(),
                                       [slicer.dimensions.political_party])

        self.assertListEqual(list(result.index), ['d', 'i', 'r', ''])

    def test_convert_cat_totals(self):
        result = clean_and_apply_index(cat_dim_nans_df.reset_index(),
                                       [slicer.dimensions.political_party.rollup()])

        self.assertListEqual(list(result.index), ['d', 'i', 'r', 'Totals'])

    def test_convert_numeric_values_to_string(self):
        result = clean_and_apply_index(uni_dim_df.reset_index(), [slicer.dimensions.candidate])
        self.assertEqual(result.index.dtype.type, np.object_)

    def test_set_uni_dim_index(self):
        result = clean_and_apply_index(uni_dim_df.reset_index(),
                                       [slicer.dimensions.candidate])

        self.assertListEqual(list(result.index), [str(x + 1) for x in range(11)])

    def test_set_uni_dim_index_with_nan_converted_to_empty_str(self):
        result = clean_and_apply_index(uni_dim_nans_df.reset_index(),
                                       [slicer.dimensions.candidate])

        self.assertListEqual(list(result.index), [str(x + 1) for x in range(11)] + [''])

    def test_convert_uni_totals(self):
        result = clean_and_apply_index(uni_dim_nans_df.reset_index(),
                                       [slicer.dimensions.candidate.rollup()])

        self.assertListEqual(list(result.index), [str(x + 1) for x in range(11)] + ['Totals'])

    def test_set_index_for_multiindex_with_nans_and_totals(self):
        result = clean_and_apply_index(cont_uni_dim_nans_totals_df.reset_index(),
                                       [slicer.dimensions.timestamp, slicer.dimensions.state.rollup()])

        self.assertListEqual(list(result.index.levels[1]), ['', '1', '2', 'Totals'])
