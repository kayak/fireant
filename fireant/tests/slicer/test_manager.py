# coding: utf-8
from unittest import TestCase

from mock import patch, MagicMock

from fireant.slicer import Slicer
from fireant.slicer.managers import SlicerManager
from fireant.slicer.transformers import *
from pypika import Table


class ManagerInitializationTests(TestCase):
    def setUp(self):
        self.slicer = Slicer(Table('test'))

    def test_transformers(self):
        self.assertTrue(hasattr(self.slicer, 'manager'))

        self.assertTrue(hasattr(self.slicer, 'highcharts'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'line_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'column_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'bar_chart'))

        self.assertTrue(hasattr(self.slicer, 'datatables'))
        self.assertTrue(hasattr(self.slicer.datatables, 'row_index_table'))
        self.assertTrue(hasattr(self.slicer.datatables, 'column_index_table'))
        self.assertTrue(hasattr(self.slicer.datatables, 'row_index_csv'))
        self.assertTrue(hasattr(self.slicer.datatables, 'column_index_csv'))

        # False until the feature can be fully added
        self.assertFalse(hasattr(self.slicer, 'notebook'))

    @patch('fireant.settings.database')
    def test_data(self, mock_db):
        self.slicer.manager.query_schema = MagicMock()
        self.slicer.manager._query_data = MagicMock()

        mock_args = {'metrics': 0, 'dimensions': 1,
                     'metric_filters': 2, 'dimension_filters': 3,
                     'references': 4, 'operations': 5,}
        self.slicer.manager.query_schema.return_value = {'a': 1, 'b': 2}
        self.slicer.manager._query_data.return_value = 'OK'

        result = self.slicer.manager.data(**mock_args)

        self.assertEqual('OK', result)
        self.slicer.manager.query_schema.assert_called_once_with(**mock_args)

    @patch.object(SlicerManager, 'display_schema')
    @patch.object(SlicerManager, 'data')
    def _test_transform(self, test_func, mock_transform, mock_sm_data, mock_sm_ds):
        mock_df, mock_schema, mock_return = 'dataframe', 'schema', 'OK'
        mock_sm_data.return_value = mock_df
        mock_sm_ds.return_value = mock_schema
        mock_transform.return_value = mock_return

        mock_args = {'metrics': 0, 'dimensions': 1,
                     'metric_filters': 2, 'dimension_filters': 3,
                     'references': 4, 'operations': 5,}

        result = test_func(**mock_args)

        self.assertEqual(mock_return, result)
        mock_sm_data.assert_called_once_with(**mock_args)
        mock_sm_ds.assert_called_once_with(mock_args['metrics'], mock_args['dimensions'], mock_args['references'])
        mock_transform.assert_called_once_with(mock_df, mock_schema)

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_line_chart(self, mock_transform):
        self._test_transform(self.slicer.highcharts.line_chart, mock_transform)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_column_chart(self, mock_transform):
        self._test_transform(self.slicer.highcharts.column_chart, mock_transform)

    @patch.object(HighchartsBarTransformer, 'transform')
    def test_transform_highcharts_bar_chart(self, mock_transform):
        self._test_transform(self.slicer.highcharts.bar_chart, mock_transform)

    @patch.object(DataTablesRowIndexTransformer, 'transform')
    def test_transform_datatables_row_index_table(self, mock_transform):
        self._test_transform(self.slicer.datatables.row_index_table, mock_transform)

    @patch.object(DataTablesColumnIndexTransformer, 'transform')
    def test_transform_datatables_col_index_table(self, mock_transform):
        self._test_transform(self.slicer.datatables.column_index_table, mock_transform)

    @patch.object(CSVRowIndexTransformer, 'transform')
    def test_transform_datatables_row_index_table(self, mock_transform):
        self._test_transform(self.slicer.datatables.row_index_csv, mock_transform)

    @patch.object(CSVColumnIndexTransformer, 'transform')
    def test_transform_datatables_col_index_table(self, mock_transform):
        self._test_transform(self.slicer.datatables.column_index_csv, mock_transform)
