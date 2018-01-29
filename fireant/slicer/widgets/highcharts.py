import itertools

import pandas as pd

from fireant import utils
from fireant.utils import immutable
from .base import (
    MetricsWidget,
    Widget,
)
from .formats import (
    dimension_value,
    metric_value,
)
from .helpers import (
    dimensional_metric_label,
    extract_display_values,
    reference_key,
)
from ..exceptions import MetricRequiredException

DEFAULT_COLORS = (
    "#DDDF0D",
    "#55BF3B",
    "#DF5353",
    "#7798BF",
    "#AAEEEE",
    "#FF0066",
    "#EEAAEE",
    "#DF5353",
    "#7798BF",
    "#AAEEEE",
)

DASH_STYLES = (
    'Solid',
    'ShortDash',
    'ShortDot',
    'ShortDashDot',
    'ShortDashDotDot',
    'Dot',
    'Dash',
    'LongDash',
    'DashDot',
    'LongDashDot',
    'LongDashDotDot',
)

MARKER_SYMBOLS = (
    "circle",
    "square",
    "diamond",
    "triangle",
    "triangle-down",
)


class ChartWidget(MetricsWidget):
    type = None
    needs_marker = False
    stacked = False

    def __init__(self, metrics=(), name=None, stacked=False):
        super(ChartWidget, self).__init__(metrics=metrics)
        self.name = name
        self.stacked = self.stacked or stacked


class HighCharts(Widget):
    class PieChart(ChartWidget):
        type = 'pie'

    class LineChart(ChartWidget):
        type = 'line'
        needs_marker = True

    class AreaChart(ChartWidget):
        type = 'area'
        needs_marker = True

    class BarChart(ChartWidget):
        type = 'bar'

    class ColumnChart(ChartWidget):
        type = 'column'

    class StackedBarChart(BarChart):
        stacked = True

    class StackedColumnChart(ColumnChart):
        stacked = True

    class AreaPercentageChart(AreaChart):
        stacked = True

    def __init__(self, title=None, axes=(), colors=None):
        self.title = title
        self.axes = list(axes)
        self.colors = colors or DEFAULT_COLORS

    @immutable
    def axis(self, axis: ChartWidget):
        """
        (Immutable) Adds an axis to the Chart.

        :param axis:
        :return:
        """

        self.axes.append(axis)

    @property
    def metrics(self):
        """
        :return:
            A set of metrics used in this chart. This collects all metrics across all axes.
        """
        if 0 == len(self.axes):
            raise MetricRequiredException(str(self))

        seen = set()
        return [metric
                for axis in self.axes
                for metric in axis.metrics
                if not (metric.key in seen or seen.add(metric.key))]

    def transform(self, data_frame, slicer, dimensions):
        """
        - Main entry point -

        Transforms a data frame into highcharts JSON format.

        See https://api.highcharts.com/highcharts/

        :param data_frame:
            The data frame containing the data. Index must match the dimensions parameter.
        :param slicer:
            The slicer that is in use.
        :param dimensions:
            A list of dimensions that are being rendered.
        :return:
            A dict meant to be dumped as JSON.
        """
        colors = itertools.cycle(self.colors)

        levels = data_frame.index.names[1:]
        groups = list(data_frame.groupby(level=levels)) \
            if levels \
            else [([], data_frame)]

        dimension_display_values = extract_display_values(dimensions, data_frame)
        render_series_label = dimensional_metric_label(dimensions, dimension_display_values)

        references = [reference
                      for dimension in dimensions
                      for reference in getattr(dimension, 'references', ())]

        y_axes, series = [], []
        for axis_idx, axis in enumerate(self.axes):
            colors, series_colors = itertools.tee(colors)
            axis_color = next(colors) if 1 < len(self.metrics) else None

            # prepend axes, append series, this keeps everything ordered left-to-right
            y_axes[0:0] = self._render_y_axis(axis_idx,
                                              axis_color,
                                              references)
            series += self._render_series(axis,
                                          axis_idx,
                                          axis_color,
                                          series_colors,
                                          groups,
                                          render_series_label,
                                          references)

        x_axis = self._render_x_axis(data_frame, dimension_display_values)

        return {
            "title": {"text": self.title},
            "xAxis": x_axis,
            "yAxis": y_axes,
            "series": series,
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
        }

    @staticmethod
    def _render_x_axis(data_frame, dimension_display_values):
        """
        Renders the xAxis configuraiton.

        https://api.highcharts.com/highcharts/yAxis

        :param data_frame:
        :param dimension_display_values:
        :return:
        """
        first_level = data_frame.index.levels[0] \
            if isinstance(data_frame.index, pd.MultiIndex) \
            else data_frame.index

        if isinstance(first_level, pd.DatetimeIndex):
            return {"type": "datetime"}

        categories = ["All"] \
            if isinstance(first_level, pd.RangeIndex) \
            else [utils.deep_get(dimension_display_values,
                                 [first_level.name, dimension_value],
                                 dimension_value)
                  for dimension_value in first_level]

        return {
            "type": "category",
            "categories": categories,
        }

    @staticmethod
    def _render_y_axis(axis_idx, color, references):
        """
        Renders the yAxis configuration.

        https://api.highcharts.com/highcharts/yAxis

        :param axis_idx:
        :param color:
        :param references:
        :return:
        """
        y_axes = [{
            "id": str(axis_idx),
            "title": {"text": None},
            "labels": {"style": {"color": color}}
        }]

        y_axes += [{
                       "id": "{}_{}".format(axis_idx, reference.key),
                       "title": {"text": reference.label},
                       "opposite": True,
                       "labels": {"style": {"color": color}}
                   }
                   for reference in references
                   if reference.is_delta]

        return y_axes

    def _render_series(self, axis, axis_idx, axis_color, colors, data_frame_groups, render_series_label, references):
        """
        Renders the series configuration.

        https://api.highcharts.com/highcharts/series

        :param axis:
        :param axis_idx:
        :param axis_color:
        :param colors:
        :param data_frame_groups:
        :param render_series_label:
        :param references:
        :return:
        """
        has_multi_metric = 1 < len(axis.metrics)

        series = []
        for metric in axis.metrics:
            visible = True
            symbols = itertools.cycle(MARKER_SYMBOLS)
            series_color = next(colors) if has_multi_metric else None

            for (dimension_values, group_df), symbol in zip(data_frame_groups, symbols):
                dimension_values = utils.wrap_list(dimension_values)

                is_timeseries = isinstance(group_df.index.levels[0]
                                           if isinstance(group_df.index, pd.MultiIndex)
                                           else group_df.index, pd.DatetimeIndex)

                if not has_multi_metric:
                    series_color = next(colors)

                for reference, dash_style in zip([None] + references, itertools.cycle(DASH_STYLES)):
                    metric_key = reference_key(metric, reference)

                    series.append({
                        "type": axis.type,
                        "color": series_color,
                        "dashStyle": dash_style,
                        "visible": visible,

                        "name": render_series_label(metric, reference, dimension_values),

                        "data": self._render_data(group_df, metric_key, is_timeseries),

                        "yAxis": ("{}_{}".format(axis_idx, reference.key)
                                  if reference is not None and reference.is_delta
                                  else str(axis_idx)),

                        "marker": ({"symbol": symbol, "fillColor": axis_color or series_color}
                                   if axis.needs_marker
                                   else {}),

                        "stacking": ("normal"
                                     if axis.stacked
                                     else None),
                    })

                visible = False  # Only display the first in each group

        return series

    def _render_data(self, group_df, metric_key, is_timeseries):
        if is_timeseries:
            return [[dimension_value(utils.wrap_list(dimension_values)[0],
                                     str_date=False),
                     metric_value(y)]
                    for dimension_values, y in group_df[metric_key].iteritems()]

        return [metric_value(y)
                for y in group_df[metric_key].values]
