# coding: utf-8
from datetime import date
from unittest import TestCase

import pandas as pd
from mock import Mock, patch, call

from fireant.dashboards import *
from fireant.slicer import *
from fireant.slicer.references import WoW
from fireant.slicer.transformers import TransformationException
from fireant.tests.database.mock_database import TestDatabase
from pypika import Table, functions as fn


class DashboardTests(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        test_table = Table('test_table')
        cls.test_slicer = Slicer(
            table=test_table,
            database=TestDatabase(),

            metrics=[
                # Metric with defaults
                Metric('clicks', 'Clicks'),
                Metric('conversions', 'Conversions'),
                Metric('roi', 'ROI', definition=fn.Sum(test_table.revenue) / fn.Sum(test_table.cost)),
                Metric('rpc', 'RPC', definition=fn.Sum(test_table.revenue) / fn.Sum(test_table.clicks)),
                Metric('cpc', 'CPC', definition=fn.Sum(test_table.cost) / fn.Sum(test_table.clicks)),
            ],

            dimensions=[
                # Continuous date dimension
                DatetimeDimension('date', definition=test_table.dt),

                # Continuous integer dimension
                ContinuousDimension('clicks', label='Clicks CUSTOM LABEL', definition=test_table.clicks),

                # Categorical dimension with fixed options
                CategoricalDimension('locale', 'Locale', definition=test_table.locale,
                                     display_options=[DimensionValue('us', 'United States'),
                                                      DimensionValue('de', 'Germany')]),

                # Unique Dimension with single ID field
                UniqueDimension('account', 'Account', definition=test_table.account_id,
                                display_field=test_table.account_name),
            ]
        )

        cls.test_slicer.manager.data = Mock()
        cls.test_slicer.manager.display_schema = Mock()

    def assert_slicer_queried(self, metrics, dimensions=None, mfilters=None, dfilters=None,
                              references=None, operations=None):
        self.test_slicer.manager.data.assert_called_with(
            metrics=metrics,
            dimensions=dimensions or [],
            metric_filters=mfilters or [],
            dimension_filters=dfilters or [],
            references=references or [],
            operations=operations or [],
        )

    def assert_result_transformed(self, widgets, dimensions, mock_transformer, tx_generator, references=(),
                                  operations=()):
        # Assert that there is a result for each widget
        self.assertEqual(len(widgets), len(list(tx_generator)))

        self.test_slicer.manager.display_schema.assert_has_calls(
            [call(
                dimensions=dimensions or [],
                metrics=widget.metrics,
                references=list(references),
                operations=list(operations),
            ) for widget in widgets]
        )

        # Assert that a transformation was performed for each widget
        self.assertEqual(len(mock_transformer.transform.call_args), len(widgets))

        for idx, widget in enumerate(widgets):
            self.assertIsInstance(mock_transformer.transform.call_args_list[idx][0][0], pd.DataFrame)

            metrics = widget.metrics
            if references:
                metrics = [(reference.key if reference else '', metric)
                           for reference in [None] + references
                           for metric in metrics]

            self.assertListEqual(list(mock_transformer.transform.call_args_list[idx][0][0].columns), metrics)


class DashboardSchemaTests(DashboardTests):
    def test_if_the_dashboard_query_string_is_equal_to_its_slicer_query_string_given_same_metrics(self):
        metrics = ['clicks']

        test_render = WidgetGroup(
            slicer=self.test_slicer,
            widgets=[
                LineChartWidget(metrics=metrics),
            ]
        )

        result = test_render.manager.query_string()

        self.assertEquals(self.test_slicer.manager.query_string(metrics=metrics), result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_metric_widgets(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = []
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ]
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            metrics,
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_categorical_dim(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['locale']
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_datetime_dim(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = [('date', DatetimeDimension.week)]
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer,
                                       result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_eq_filter_dim(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = []
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        eq_filter = EqualityFilter('device_type', EqualityOperator.eq, 'desktop')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimension_filters=[eq_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            metrics,
            dfilters=[eq_filter],
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_contains_filter_dim(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = []
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        contains_filter = ContainsFilter('device_type', ['desktop', 'mobile'])
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimension_filters=[contains_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            metrics,
            dfilters=[contains_filter],
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_range_filter_date(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = []
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        range_filter = RangeFilter('date', date(2000, 1, 1), date(2000, 3, 1))
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimension_filters=[range_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            metrics,
            dfilters=[range_filter],
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_wildcard_filter_date(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = []
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        wildcard_filter = WildcardFilter('locale', 'U%')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimension_filters=[wildcard_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            metrics,
            dfilters=[wildcard_filter],
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_reference_with_dim(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['date']
        references = [WoW('date')]
        self.test_slicer.manager.data.return_value = pd.DataFrame(
            columns=pd.MultiIndex.from_product([['', 'wow'], metrics])
        )
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': {'wow': 'date'}},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': {'wow': 'date'}},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(references=references)

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
            references=references,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result, references)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_reference_in_widgetgroup(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['date']
        references = [WoW('date')]
        self.test_slicer.manager.data.return_value = pd.DataFrame(
            columns=pd.MultiIndex.from_product([['', 'wow'], metrics])
        )
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': {'wow': 'date'}},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': {'wow': 'date'}},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
            references=references,
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
            references=references,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result, references)


class DashboardAPITests(DashboardTests):
    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_api_with_dimension(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['date']
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ]
        )

        result = test_render.manager.render(
            dimensions=dimensions,
        )

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_api_with_filter(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = []
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        eq_filter = EqualityFilter('device_type', EqualityOperator.eq, 'desktop')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ]
        )

        result = test_render.manager.render(
            dimension_filters=[eq_filter],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dfilters=[eq_filter],
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_api_with_reference(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['date']
        self.test_slicer.manager.data.return_value = pd.DataFrame(
            columns=pd.MultiIndex.from_product([['', 'wow'], metrics])
        )
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': {}},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': {}},
        ]

        references = [WoW('date')]
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions
        )

        result = test_render.manager.render(
            references=references,
        )

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
            references=references,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result, references)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['date']
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(
            dimensions=dimensions,
        )

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys_with_intervals_in_schema(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = [('date', DatetimeDimension.day)]
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(
            dimensions=['date'],
        )

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys_with_intervals_in_api(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = [('date', DatetimeDimension.day)]
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(
            dimensions=[('date', DatetimeDimension.week)],
        )

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys_with_intervals_in_api2(self, mock_transformer):
        metrics = ['clicks', 'conversions']
        dimensions = ['date']
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=metrics)
        self.test_slicer.manager.display_schema.side_effect = [
            {'metrics': metrics[:1], 'dimensions': dimensions, 'references': []},
            {'metrics': metrics[1:], 'dimensions': dimensions, 'references': []},
        ]

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=metrics[:1]),
                LineChartWidget(metrics=metrics[1:]),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(
            dimensions=[('date', DatetimeDimension.week)],
        )

        self.assert_slicer_queried(
            metrics,
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)


class PrevalidationTests(DashboardTests):
    @classmethod
    def setUpClass(cls):
        super(PrevalidationTests, cls).setUpClass()

        cls.test_wg = WidgetGroup(
            slicer=cls.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ]
        )

    def test_raises_exception_for_linechart_with_no_dimensions(self):
        with self.assertRaises(TransformationException):
            self.test_wg.manager.render(dimensions=[])

    @patch('fireant.dashboards.LineChartWidget.transformer.transform')
    def test_raises_exception_for_linechart_without_continuous_first_dimension(self, mock_transform):
        self.test_slicer.manager.data.return_value = pd.DataFrame(columns=['clicks', 'conversions'])

        self.test_wg.manager.render(dimensions=['date'])
        self.test_wg.manager.render(dimensions=['clicks'])

        with self.assertRaises(TransformationException):
            self.test_wg.manager.render(dimensions=['locale'])
        with self.assertRaises(TransformationException):
            self.test_wg.manager.render(dimensions=['account'])
