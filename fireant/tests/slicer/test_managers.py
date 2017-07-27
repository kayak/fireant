# coding: utf-8
import copy
import itertools
from unittest import TestCase

import pandas as pd
from fireant.slicer import *
from fireant.slicer.managers import SlicerManager
from fireant.slicer.operations import CumSum
from fireant.slicer.references import WoW
from fireant.slicer.transformers import *
from fireant.tests.database.mock_database import TestDatabase
from mock import (
    MagicMock,
    patch,
)
from pypika import (
    Table,
    Query,
)


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
        self.paginator = Paginator(offset=10, limit=10)

    def test_transformers(self):
        self.assertTrue(hasattr(self.slicer, 'manager'))

        self.assertTrue(hasattr(self.slicer, 'notebooks'))
        self.assertTrue(hasattr(self.slicer.notebooks, 'line_chart'))
        self.assertTrue(hasattr(self.slicer.notebooks, 'bar_chart'))

        self.assertTrue(hasattr(self.slicer, 'highcharts'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'line_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'area_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'column_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'bar_chart'))
        self.assertTrue(hasattr(self.slicer.highcharts, 'pie_chart'))

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
        mock_args = {'metrics': ['a'], 'dimensions': ['e'],
                     'metric_filters': [2], 'dimension_filters': [3],
                     'references': [WoW('d')], 'operations': [5], 'pagination': None}
        mock_query_schema.return_value = {'a': 1, 'b': 2}
        mock_query_data.return_value = mock_post_process.return_value = pd.DataFrame(
                columns=itertools.product(['', 'wow'], ['a', 'c', 'm_test']))
        mock_operation_schema.return_value = [{'metric': 'm', 'key': 'test'}]

        result = self.slicer.manager.data(**mock_args)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertListEqual([('', 'a'), ('', 'm_test'), ('wow', 'a'), ('wow', 'm_test')], list(result.columns))
        mock_query_schema.assert_called_once_with(**mock_args)
        mock_query_data.assert_called_once_with(a=1, b=2)
        mock_operation_schema.assert_called_once_with(mock_args['operations'])

    @patch('fireant.slicer.managers.SlicerManager._build_data_query')
    @patch('fireant.slicer.managers.SlicerManager.data_query_schema')
    def test_query_string(self, mock_query_schema, mock_build_query_string):
        mock_args = {'metrics': [0], 'dimensions': [1],
                     'metric_filters': [2], 'dimension_filters': [3],
                     'references': [4], 'operations': [5], 'pagination': self.paginator}
        mock_query_schema.return_value = {'database': 'db1', 'a': 1, 'b': 2}
        mock_build_query_string.return_value = 1

        result = self.slicer.manager.query_string(**mock_args)

        self.assertEqual(str(mock_build_query_string.return_value), result)
        mock_query_schema.assert_called_once_with(**mock_args)
        mock_build_query_string.assert_called_once_with(database='db1', a=1, b=2)

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
        mock_transform.assert_called_once_with(mock_df, mock_schema)

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_line_chart(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cont'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.line_chart, mock_transform, request)

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_line_chart_date(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['date'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
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

    @patch.object(HighchartsLineTransformer, 'transform')
    def test_transform_highcharts_require_cont_dim_on_slicer_with_no_dims(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': [],
        }

        slicer = copy.deepcopy(self.slicer)
        slicer.dimensions = []
        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.line_chart, mock_transform, request)

    @patch.object(HighchartsAreaTransformer, 'transform')
    def test_transform_highcharts_area_chart(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cont'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.area_chart, mock_transform, request)

    @patch.object(HighchartsAreaTransformer, 'transform')
    def test_transform_highcharts_area_chart_date(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['date'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.area_chart, mock_transform, request)

    @patch.object(HighchartsAreaTransformer, 'transform')
    def test_transform_highcharts_area_chart_require_cont_dim(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.area_chart, mock_transform, request)

    @patch.object(HighchartsAreaTransformer, 'transform')
    def test_transform_highcharts_area_chart_require_cont_dim_on_slicer_with_no_dims(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': [],
        }

        slicer = copy.deepcopy(self.slicer)
        slicer.dimensions = []
        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.area_chart, mock_transform, request)

    @patch.object(HighchartsAreaPercentageTransformer, 'transform')
    def test_transform_highcharts_area_percentage_chart(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cont'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.area_percentage_chart, mock_transform, request)

    @patch.object(HighchartsAreaPercentageTransformer, 'transform')
    def test_transform_highcharts_area_percentage_chart_date(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['date'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.area_percentage_chart, mock_transform, request)

    @patch.object(HighchartsAreaPercentageTransformer, 'transform')
    def test_transform_highcharts_area_chart_require_cont_dim(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.area_percentage_chart, mock_transform, request)

    @patch.object(HighchartsAreaPercentageTransformer, 'transform')
    def test_transform_highcharts_area_percentage_chart_require_cont_dim_on_slicer_with_no_dims(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': [],
        }

        slicer = copy.deepcopy(self.slicer)
        slicer.dimensions = []
        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.area_percentage_chart, mock_transform, request)

    @patch.object(HighchartsPieTransformer, 'transform')
    def test_transform_highcharts_pie_chart(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': ['cont'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.pie_chart, mock_transform, request)

    @patch.object(HighchartsPieTransformer, 'transform')
    def test_transform_highcharts_pie_chart_date(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': ['date'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }
        self._test_transform(self.slicer.highcharts.pie_chart, mock_transform, request)

    @patch.object(HighchartsPieTransformer, 'transform')
    def test_transform_highcharts_pie_chart_does_not_allow_two_metrics(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat'],
        }

        with self.assertRaises(TransformationException):
            self._test_transform(self.slicer.highcharts.pie_chart, mock_transform, request)

    @patch.object(HighchartsPieTransformer, 'transform')
    def test_transform_highcharts_pie_chart_multiple_dimensions_one_metric(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': ['date', 'cat'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }

        self._test_transform(self.slicer.highcharts.pie_chart, mock_transform, request)

    @patch.object(HighchartsColumnTransformer, 'transform')
    def test_transform_highcharts_column_chart(self, mock_transform):
        request = {
            'metrics': ['foo'],
            'dimensions': [],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
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
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
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
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }

        self._test_transform(self.slicer.datatables.row_index_table, mock_transform, request)

    @patch.object(DataTablesColumnIndexTransformer, 'transform')
    def test_transform_datatables_col_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }

        self._test_transform(self.slicer.datatables.column_index_table, mock_transform, request)

    @patch.object(CSVRowIndexTransformer, 'transform')
    def test_transform_datatables_row_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
        }

        self._test_transform(self.slicer.datatables.row_index_csv, mock_transform, request)

    @patch.object(CSVColumnIndexTransformer, 'transform')
    def test_transform_datatables_col_index_table(self, mock_transform):
        request = {
            'metrics': ['foo', 'bar'],
            'dimensions': ['cat', 'uni'],
            'metric_filters': (), 'dimension_filters': (),
            'references': (), 'operations': (), 'pagination': self.paginator,
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
                references=(), operations=(), pagination=None
        )

    @patch.object(SlicerManager, 'query_data')
    @patch.object(SlicerManager, 'data_query_schema')
    def test_slicer_exception_raised_with_operations_and_pagination(self, mock_query_schema, mock_query_data):
        with self.assertRaises(SlicerException):
            self.slicer.manager.data(
                    metrics=['foo', 'foo'],
                    operations=[CumSum('foo')],
                    pagination=Paginator(offset=10, limit=10)
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
                references=(), operations=(), pagination=None
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
                references=(), operations=(), pagination=None
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
                references=(), operations=(), pagination=None
        )

    @patch.object(TestDatabase, 'fetch_dataframe')
    @patch('fireant.slicer.queries.query_logger')
    def test_get_dataframe_from_query_logs_query_before_and_query_after_with_duration(self, mock_logging, *args):
        query = Query.from_('customers').select('id')
        self.slicer.manager._get_dataframe_from_query(TestDatabase(), query)
        self.assertEqual(mock_logging.debug.call_count, 1)
        self.assertEqual(mock_logging.info.call_count, 1)

    @patch.object(SlicerManager, '_get_dataframe_from_query')
    @patch.object(SlicerManager, '_build_data_query')
    def test_query_data_calls_build_data_query_with_correct_args(self, mock_build_query, *args):
        db = TestDatabase()
        metrics = {'bar': self.slicer.metrics.get('bar')}
        dimensions = {'cat': self.slicer.dimensions.get('cat')}
        dfilters = ContainsFilter('cat', ['a', 'b'])
        references = {'date': WoW('date')}
        args = {
            'database': db,
            'table': self.slicer.table,
            'joins': self.slicer.joins,
            'metrics': metrics,
            'dimensions': dimensions,
            'dfilters': dfilters,
            'references': references
        }
        self.slicer.manager.query_data(**args)

        mock_build_query.assert_called_once_with(db, self.slicer.table, self.slicer.joins,
                                                 metrics, dimensions, dfilters, None, references, None, None)

    @patch.object(SlicerManager, '_get_dataframe_from_query')
    @patch.object(SlicerManager, '_build_data_query')
    def test_query_data_calls_get_dataframe_from_query_with_correct_args(self, mock_query, mock_get_dataframe):
        query = Query.from_('customers').select('id')
        mock_query.return_value = query
        db = TestDatabase()
        self.slicer.manager.query_data(db, self.slicer.table)
        mock_get_dataframe.assert_called_once_with(db, query)
