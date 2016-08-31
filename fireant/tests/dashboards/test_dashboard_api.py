# coding: utf-8
from datetime import date
from unittest import TestCase

from mock import Mock, MagicMock, patch, call, ANY

from fireant.dashboards import *
from fireant.slicer import *
from fireant.slicer.references import WoW
from pypika import Table, functions as fn


class DashboardTests(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        test_table = Table('test_table')
        cls.test_slicer = Slicer(
            test_table,

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

        cls.mock_dataframe = MagicMock()
        cls.test_slicer.manager.data = Mock(return_value=cls.mock_dataframe)

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

    def assert_result_transformed(self, widgets, dimensions, mock_transformer, tx_generator):
        # Assert that there is a result for each widget
        self.assertEqual(len(widgets), len(list(tx_generator)))

        self.test_slicer.manager.display_schema.assert_has_calls(
            [call(
                dimensions=dimensions or [],
                metrics=widget.metrics,
            ) for widget in widgets]
        )

        # Assert that dataframe was sliced for each widget with the right metrics
        self.mock_dataframe.__getitem__.assert_has_calls(
            [call(widget.metrics)
             for widget in widgets],
            any_order=True
        )

        # Assert that a transformation was performed for each widget
        mock_transformer.transform.assert_has_calls(
            [call(
                self.mock_dataframe[widget.metrics].__getitem__(),
                ANY
            ) for widget in widgets],
            any_order=True
        )


class DashboardSchemaTests(DashboardTests):
    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_metric_widgets(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': []})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ]
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_categorical_dim(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': ['locale']})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=['locale'],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=['locale'],
        )
        self.assert_result_transformed(test_render.widgets, ['locale'], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_datetime_dim(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': [('date', DatetimeDimension.week)]})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=[('date', DatetimeDimension.week)],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=[('date', DatetimeDimension.week)],
        )
        self.assert_result_transformed(test_render.widgets, [('date', DatetimeDimension.week)], mock_transformer,
                                       result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_eq_filter_dim(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': []})

        eq_filter = EqualityFilter('device_type', EqualityOperator.eq, 'desktop')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimension_filters=[eq_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dfilters=[eq_filter],
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_contains_filter_dim(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': []})

        contains_filter = ContainsFilter('device_type', ['desktop', 'mobile'])
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimension_filters=[contains_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dfilters=[contains_filter],
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_range_filter_date(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': []})

        range_filter = RangeFilter('date', date(2000, 1, 1), date(2000, 3, 1))
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimension_filters=[range_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dfilters=[range_filter],
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_wildcard_filter_date(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': []})

        wildcard_filter = WildcardFilter('locale', 'U%')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimension_filters=[wildcard_filter],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dfilters=[wildcard_filter],
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_reference_with_dim(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': ['date']})

        reference = WoW('date')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=['date'],

            references=[reference],
        )

        result = test_render.manager.render()

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=['date'],
            references=[reference],
        )
        self.assert_result_transformed(test_render.widgets, ['date'], mock_transformer, result)


class DashboardAPITests(DashboardTests):
    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_api_with_dimension(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': ['date']})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ]
        )

        result = test_render.manager.render(
            dimensions=['date'],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=['date'],
        )
        self.assert_result_transformed(test_render.widgets, ['date'], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_api_with_filter(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': []})

        eq_filter = EqualityFilter('device_type', EqualityOperator.eq, 'desktop')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ]
        )

        result = test_render.manager.render(
            dimension_filters=[eq_filter],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dfilters=[eq_filter],
        )
        self.assert_result_transformed(test_render.widgets, [], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_api_with_reference(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': ['date']})

        reference = WoW('date')
        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=[
                'date'
            ]
        )

        result = test_render.manager.render(
            references=[reference],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=['date'],
            references=[reference],
        )
        self.assert_result_transformed(test_render.widgets, ['date'], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': ['date']})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=['date'],
        )

        result = test_render.manager.render(
            dimensions=['date'],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=['date'],
        )
        self.assert_result_transformed(test_render.widgets, ['date'], mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys_with_intervals_in_schema(self, mock_transformer):
        dimensions = [('date', DatetimeDimension.day)]
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': dimensions})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(
            dimensions=['date'],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys_with_intervals_in_api(self, mock_transformer):
        dimensions = [('date', DatetimeDimension.day)]
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': dimensions})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=dimensions,
        )

        result = test_render.manager.render(
            dimensions=[('date', DatetimeDimension.week)],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=dimensions,
        )
        self.assert_result_transformed(test_render.widgets, dimensions, mock_transformer, result)

    @patch('fireant.dashboards.LineChartWidget.transformer')
    def test_remove_duplicated_dimension_keys_with_intervals_in_api2(self, mock_transformer):
        self.test_slicer.manager.display_schema = Mock(return_value={'metrics': ['clicks', 'conversions'],
                                                                     'dimensions': ['date']})

        test_render = WidgetGroup(
            slicer=self.test_slicer,

            widgets=[
                LineChartWidget(metrics=['clicks']),
                LineChartWidget(metrics=['conversions']),
            ],

            dimensions=['date'],
        )

        result = test_render.manager.render(
            dimensions=[('date', DatetimeDimension.week)],
        )

        self.assert_slicer_queried(
            ['clicks', 'conversions'],
            dimensions=['date'],
        )
        self.assert_result_transformed(test_render.widgets, ['date'], mock_transformer, result)
