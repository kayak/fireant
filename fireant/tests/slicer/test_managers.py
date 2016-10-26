# coding: utf-8
from unittest import TestCase

from mock import patch, MagicMock

from fireant.slicer import *
from fireant.slicer.managers import SlicerManager
from fireant.slicer.transformers import *
from fireant.tests.database.mock_database import TestDatabase
from pypika import Table


class ManagerInitializationTests(TestCase):
    def setUp(self):
        self.test_table = Table('test')
        self.test_database = TestDatabase()
        self.slicer = Slicer(
            self.test_table,
            self.test_database,

            metrics=[
                Metric('foo'),
                Metric('bar'),
            ],

            dimensions=[
                ContinuousDimension('cont'),
                DatetimeDimension('date'),
                CategoricalDimension('cat'),
                UniqueDimension('uni', display_field=self.test_table.uni_label),
            ]
        )

    def test_transformers(self):
        self.assertTrue(hasattr(self.slicer, 'manager'))

        self.assertTrue(hasattr(self.slicer, 'notebooks'))
        self.assertTrue(hasattr(self.slicer.notebooks, 'line_chart'))
        self.assertTrue(hasattr(self.slicer.notebooks, 'bar_chart'))

        self.assertTrue(hasattr(self.slicer, 'highcharts'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'line_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'column_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'bar_chart'))

        self.assertTrue(hasattr(self.slicer, 'datatables'))
        self.assertTrue(hasattr(self.slicer.datatables, 'row_index_table'))
        self.assertTrue(hasattr(self.slicer.datatables, 'column_index_table'))
        self.assertTrue(hasattr(self.slicer.datatables, 'row_index_csv'))
        self.assertTrue(hasattr(self.slicer.datatables, 'column_index_csv'))

    @patch('fireant.slicer.managers.SlicerManager.post_process')
    @patch('fireant.slicer.managers.SlicerManager.query_data')
    @patch('fireant.slicer.managers.SlicerManager.operation_schema')
    @patch('fireant.slicer.managers.SlicerManager.data_query_schema')
    def test_data(self, mock_query_schema, mock_operation_schema, mock_query_data, mock_post_process):
        mock_args = {'metrics': [0], 'dimensions': [1],
                     'metric_filters': [2], 'dimension_filters': [3],
                     'references': [4], 'operations': [5]}
        mock_query_schema.return_value = {'a': 1, 'b': 2}
        mock_query_data.return_value = 'dataframe'
        mock_operation_schema.return_value = 'op_schema'
        mock_post_process.return_value = 'OK'

        result = self.slicer.manager.data(**mock_args)

        self.assertEqual('OK', result)
        mock_query_schema.assert_called_once_with(**mock_args)
        mock_query_data.assert_called_once_with(a=1, b=2)
        mock_post_process.assert_called_once_with('dataframe', 'op_schema')
        mock_operation_schema.assert_called_once_with(mock_args['operations'])

    def missing_database_config(self):
        with self.assertRaises(SlicerException):
            self.slicer.manager.data()

    def test_require_metrics(self):
        with self.assertRaises(SlicerException):
            self.slicer.manager._metrics_schema([])

    def test_require_valid_metrics(self):
        with self.assertRaises(SlicerException):
            self.slicer.manager._metrics_schema(['fizz', 'buzz'])

    def test_require_valid_dimensions(self):
        with self.assertRaises(SlicerException):
            self.slicer.manager._dimensions_schema(['fizz', 'buzz'])

    @patch.object(SlicerManager, 'display_schema')
    @patch.object(SlicerManager, 'data')
    def _test_transform(self, test_func, mock_transform, request, mock_sm_data, mock_sm_ds):
        mock_sm_data.return_value = mock_df = MagicMock()
        mock_sm_ds.return_value = mock_schema = {
            'metrics': []
        }
        mock_transform.return_value = mock_return = 'OK'

        result = test_func(**request)

        self.assertEqual(mock_return, result)
        mock_sm_data.assert_called_once_with(**request)
        mock_sm_ds.assert_called_once_with(request['metrics'], request['dimensions'], request.get('references', ()), ())
        mock_transform.assert_called_once_with(mock_df.__getitem__(), mock_schema)

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_line_chart(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cont'],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }
        self._test_transform(self.slicer.highcharts.line_chart, mock_transform, request)

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_line_chart_date(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['date'],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }
        self._test_transform(self.slicer.highcharts.line_chart, mock_transform, request)

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_require_cont_dim(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.line_chart, mock_transform, request)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_column_chart(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': [],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }
        self._test_transform(self.slicer.highcharts.column_chart, mock_transform, request)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_max_dims(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': ['cat', 'uni', 'cont'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.column_chart, mock_transform, request)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_max_metrics(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.column_chart, mock_transform, request)

    @patch.object(HighchartsBarTransformer, 'transform')
    def test_transform_highcharts_bar_chart(self, mock_transform):
        request = {
            'metrics': ['foo'], 'dimensions': [],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }
        self._test_transform(self.slicer.highcharts.bar_chart, mock_transform, request)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_max_dims(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': ['cat', 'uni', 'cont'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.bar_chart, mock_transform, request)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_max_metrics(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.bar_chart, mock_transform, request)

    @patch.object(DataTablesRowIndexTransformer, 'transform')
    def test_transform_datatables_row_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }

        self._test_transform(self.slicer.datatables.row_index_table, mock_transform, request)

    @patch.object(DataTablesColumnIndexTransformer, 'transform')
    def test_transform_datatables_col_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }

        self._test_transform(self.slicer.datatables.column_index_table, mock_transform, request)

    @patch.object(CSVRowIndexTransformer, 'transform')
    def test_transform_datatables_row_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }

        self._test_transform(self.slicer.datatables.row_index_csv, mock_transform, request)

    @patch.object(CSVColumnIndexTransformer, 'transform')
    def test_transform_datatables_col_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': tuple(), 'dimension_filters': tuple(),
            'references': tuple(), 'operations': tuple(),
        }

        self._test_transform(self.slicer.datatables.column_index_csv, mock_transform, request)

    @patch.object(SlicerManager, 'query_data')
    @patch.object(SlicerManager, 'data_query_schema')
    def test_remove_duplicate_metric_keys(self, mock_query_schema, mock_query_data):
        self.slicer.manager.data(
            metrics=['foo', 'foo']
        )

        mock_query_schema.assert_called_once_with(
            metrics=['foo'],
            dimensions=[],
            metric_filters=(), dimension_filters=(),
            references=(), operations=(),
        )

    @patch.object(SlicerManager, 'query_data')
    @patch.object(SlicerManager, 'data_query_schema')
    def test_remove_duplicate_dimension_keys(self, mock_query_schema, mock_query_data):
        self.slicer.manager.data(
            metrics=['foo'],
            dimensions=['fizz', 'fizz'],
        )

        mock_query_schema.assert_called_once_with(
            metrics=['foo'],
            dimensions=['fizz'],
            metric_filters=(), dimension_filters=(),
            references=(), operations=(),
        )

    @patch.object(SlicerManager, 'query_data')
    @patch.object(SlicerManager, 'data_query_schema')
    def test_remove_duplicate_dimension_keys_with_interval(self, mock_query_schema, mock_query_data):
        self.slicer.manager.data(
            metrics=['foo'],
            dimensions=['fizz', ('fizz', DatetimeDimension.week)],
        )

        mock_query_schema.assert_called_once_with(
            metrics=['foo'],
            dimensions=['fizz'],
            metric_filters=(), dimension_filters=(),
            references=(), operations=(),
        )

    @patch.object(SlicerManager, 'query_data')
    @patch.object(SlicerManager, 'data_query_schema')
    def test_remove_duplicate_dimension_keys_with_interval_backwards(self, mock_query_schema, mock_query_data):
        mock_query_schema.reset()
        self.slicer.manager.data(
            metrics=['foo'],
            dimensions=[('fizz', DatetimeDimension.week), 'fizz'],
        )

        mock_query_schema.assert_called_once_with(
            metrics=['foo'],
            dimensions=[('fizz', DatetimeDimension.week)],
            metric_filters=(), dimension_filters=(),
            references=(), operations=(),
        )
