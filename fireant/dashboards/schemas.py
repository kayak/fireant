# coding: utf-8
from fireant.dashboards.managers import WidgetGroupManager

from fireant.slicer.transformers import *


class Widget(object):
    """
    The `Widget` class represents a chart or table widget in one of the dashboard's sections.

    Attributes:
        * label: human readable text.
        * metrics: list of metric names/identifiers
        * width
    """

    def __init__(self, metrics=tuple()):
        self.metrics = metrics


class LineChartWidget(Widget):
    """
    The `LineChartWidget` class represents a Highcharts line chart.
    """
    transformer = HighchartsLineTransformer()


class BarChartWidget(Widget):
    """
    The `BarChartWidget` class represents a Highcharts bar chart.
    """
    transformer = HighchartsBarTransformer()


class ColumnChartWidget(Widget):
    """
    The `ColumnChartWidget` class represents a Highcharts column chart.
    """
    transformer = HighchartsColumnTransformer()


class RowIndexTableWidget(Widget):
    """
    The `RowIndexTableWidget` class represents datatables.js data table with row-indexed dimensions.
    """
    transformer = DataTablesRowIndexTransformer()


class ColumnIndexTableWidget(Widget):
    """
    The `ColumnIndexTableWidget` class represents datatables.js data table with column-indexed dimensions.
    """
    transformer = DataTablesColumnIndexTransformer()


class WidgetGroup(object):
    def __init__(self, slicer, widgets=None, dimensions=None, dimension_filters=None, references=None, operations=None):
        self.slicer = slicer
        self.widgets = widgets

        self.dimensions = dimensions or []
        self.dimension_filters = dimension_filters or []
        self.references = references or []
        self.operations = operations or []

        self.manager = WidgetGroupManager(self)
