import itertools

import pandas as pd
from datetime import (
    datetime,
)
from typing import (
    Iterable,
    Union,
)

from fireant import (
    DatetimeDimension,
    Metric,
    Operation,
    formats,
    utils,
)
from .base import (
    TransformableWidget,
)
from .helpers import (
    dimensional_metric_label,
    extract_display_values,
)
from ..exceptions import MetricRequiredException
from ..references import (
    reference_key,
    reference_label,
)

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
    'Dash',
    'Dot',
    'DashDot',
    'LongDash',
    'LongDashDot',
    'ShortDash',
    'ShortDashDot',
    'LongDashDotDot',
    'ShortDashDotDot',
    'ShortDot',
)

MARKER_SYMBOLS = (
    "circle",
    "square",
    "diamond",
    "triangle",
    "triangle-down",
)


class ChartWidget:
    type = None
    needs_marker = False
    stacking = None

    def __init__(self, metric: Union[Metric, Operation], name=None, stacking=None):
        self.metric = metric
        self.name = name
        self.stacking = self.stacking or stacking

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__,
                               repr(self.metric))


class ContinuousAxisChartWidget(ChartWidget):
    pass


class HighCharts(TransformableWidget):
    class Axis:
        def __init__(self, series: Iterable[ChartWidget], y_axis_visible=True):
            self._series = series or []
            self.y_axis_visible = y_axis_visible

        def __iter__(self):
            return iter(self._series)

        def __len__(self):
            return len(self._series)

        def __repr__(self):
            return "axis({})".format(", ".join(map(repr, self)))

    class LineChart(ContinuousAxisChartWidget):
        type = 'line'
        needs_marker = True

    class AreaChart(ContinuousAxisChartWidget):
        type = 'area'
        needs_marker = True

    class AreaStackedChart(AreaChart):
        stacking = "normal"

    class AreaPercentageChart(AreaChart):
        stacking = "percent"

    class PieChart(ChartWidget):
        type = 'pie'

    class BarChart(ChartWidget):
        type = 'bar'

    class StackedBarChart(BarChart):
        stacking = "normal"

    class ColumnChart(ChartWidget):
        type = 'column'

    class StackedColumnChart(ColumnChart):
        stacking = "normal"

    def __init__(self, title=None, colors=None, x_axis_visible=True, tooltip_visible=True):
        super(HighCharts, self).__init__()
        self.title = title
        self.colors = colors or DEFAULT_COLORS
        self.x_axis_visible = x_axis_visible
        self.tooltip_visible = tooltip_visible

    def __repr__(self):
        return ".".join(["HighCharts()"] + [repr(axis) for axis in self.items])

    @utils.immutable
    def axis(self, *series: ChartWidget, **kwargs):
        """
        (Immutable) Adds an axis to the Chart.

        :param axis:
        :return:
        """

        self.items.append(self.Axis(series, **kwargs))

    @property
    def metrics(self):
        """
        :return:
            A set of metrics used in this chart. This collects all metrics across all axes.
        """
        if 0 == len(self.items):
            raise MetricRequiredException(str(self))

        seen = set()
        return [metric
                for axis in self.items
                for series in axis
                for metric in getattr(series.metric, 'metrics', [series.metric])
                if not (metric.key in seen or seen.add(metric.key))]

    @property
    def operations(self):
        return utils.ordered_distinct_list_by_attr([operation
                                                    for axis in self.items
                                                    for series in axis
                                                    if isinstance(series.metric, Operation)
                                                    for operation in [series.metric] + series.metric.operations])

    def transform(self, data_frame, slicer, dimensions, references):
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
        :param references:
            A list of references that are being rendered.
        :return:
            A dict meant to be dumped as JSON.
        """
        colors = itertools.cycle(self.colors)

        def group_series(keys):
            if isinstance(keys[0], datetime) and pd.isnull(keys[0]):
                return tuple('Totals' for _ in keys[1:])
            return tuple(str(key) if not pd.isnull(key) else 'Totals' for key in keys[1:])

        groups = list(data_frame.groupby(group_series)) \
            if isinstance(data_frame.index, pd.MultiIndex) \
            else [([], data_frame)]

        dimension_display_values = extract_display_values(dimensions, data_frame)
        render_series_label = dimensional_metric_label(dimensions, dimension_display_values)

        total_num_series = sum([len(axis)
                                for axis in self.items])

        y_axes, series = [], []
        for axis_idx, axis in enumerate(self.items):
            colors, series_colors = itertools.tee(colors)
            axis_color = next(colors) if 1 < total_num_series else None

            # prepend axes, append series, this keeps everything ordered left-to-right
            y_axes[0:0] = self._render_y_axis(axis_idx,
                                              axis_color,
                                              references)
            is_timeseries = dimensions and isinstance(dimensions[0], DatetimeDimension)
            series += self._render_series(axis,
                                          axis_idx,
                                          axis_color,
                                          series_colors,
                                          groups,
                                          render_series_label,
                                          references,
                                          is_timeseries)

        x_axis = self._render_x_axis(data_frame, dimensions, dimension_display_values)

        return {
            "title": {"text": self.title},
            "xAxis": x_axis,
            "yAxis": y_axes,
            "series": series,
            "tooltip": {"shared": True, "useHTML": True, "enabled": self.tooltip_visible},
            "legend": {"useHTML": True},
        }

    def _render_x_axis(self, data_frame, dimensions, dimension_display_values):
        """
        Renders the xAxis configuration.

        https://api.highcharts.com/highcharts/xAxis

        :param data_frame:
        :param dimension_display_values:
        :return:
        """
        first_level = data_frame.index.levels[0] \
            if isinstance(data_frame.index, pd.MultiIndex) \
            else data_frame.index

        if dimensions and isinstance(dimensions[0], DatetimeDimension):
            return {
                "type": "datetime",
                "visible": self.x_axis_visible,
            }

        categories = ["All"] \
            if isinstance(first_level, pd.RangeIndex) \
            else [utils.getdeepattr(dimension_display_values,
                                    (first_level.name, dimension_value),
                                    dimension_value or 'Totals')
                  for dimension_value in first_level]

        return {
            "type": "category",
            "categories": categories,
            "visible": self.x_axis_visible,
        }

    def _render_y_axis(self, axis_idx, color, references):
        """
        Renders the yAxis configuration.

        https://api.highcharts.com/highcharts/yAxis

        :param axis_idx:
        :param color:
        :param references:
        :return:
        """
        axis = self.items[axis_idx]

        y_axes = [{
            "id": str(axis_idx),
            "title": {"text": None},
            "labels": {"style": {"color": color}},
            "visible": axis.y_axis_visible,
        }]

        y_axes += [{
            "id": "{}_{}".format(axis_idx, reference.key),
            "title": {"text": reference.label},
            "opposite": True,
            "labels": {"style": {"color": color}},
            "visible": axis.y_axis_visible,
        }
            for reference in references
            if reference.delta]

        return y_axes

    def _render_series(self, axis, axis_idx, axis_color, colors, data_frame_groups, render_series_label,
                       references, is_timeseries=False):
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
        :param is_timeseries:
        :return:
        """
        has_multi_axis = 1 < len(axis)

        hc_series = []
        for series in axis:
            symbols = itertools.cycle(MARKER_SYMBOLS)
            series_color = next(colors) if has_multi_axis else None

            for (dimension_values, group_df), symbol in zip(data_frame_groups, symbols):
                dimension_values = utils.wrap_list(dimension_values)

                if isinstance(series, self.PieChart):
                    # pie charts suck
                    for reference in [None] + references:
                        hc_series += [self._render_pie_series(series,
                                                              reference,
                                                              dimension_values,
                                                              group_df,
                                                              render_series_label)]
                    continue

                if not has_multi_axis:
                    series_color = next(colors)

                for reference, dash_style in zip([None] + references, itertools.cycle(DASH_STYLES)):
                    metric_key = utils.format_metric_key(reference_key(series.metric, reference))

                    hc_series.append({
                        "type": series.type,
                        "color": series_color,
                        "dashStyle": dash_style,

                        "name": render_series_label(dimension_values, series.metric, reference),

                        "data": self._render_data(group_df, metric_key, is_timeseries),

                        "tooltip": self._render_tooltip(series.metric),

                        "yAxis": ("{}_{}".format(axis_idx, reference.key)
                                  if reference is not None and reference.delta
                                  else str(axis_idx)),

                        "marker": ({"symbol": symbol, "fillColor": axis_color or series_color}
                                   if series.needs_marker
                                   else {}),

                        "stacking": series.stacking,
                    })

        return hc_series

    def _render_pie_series(self, series, reference, dimension_values, data_frame, render_series_label):
        metric = series.metric
        name = reference_label(metric, reference)
        df_key = utils.format_metric_key(series.metric.key)

        return {
            "name": name,
            "type": series.type,
            "colors": list(self.colors),
            "data": [{
                "name": render_series_label(dimension_values) if dimension_values else name,
                "y": formats.metric_value(y),
            } for dimension_values, y in data_frame[df_key].iteritems()],
            'tooltip': {
                'valueDecimals': metric.precision,
                'valuePrefix': metric.prefix,
                'valueSuffix': metric.suffix,
            },
        }

    def _render_data(self, group_df, metric_key, is_timeseries):
        if not is_timeseries:
            return [formats.metric_value(y) for y in group_df[metric_key].values]

        series = []
        for dimension_values, y in group_df[metric_key].iteritems():
            first_dimension_value = utils.wrap_list(dimension_values)[0]

            if pd.isnull(first_dimension_value):
                # Ignore totals on the x-axis.
                continue

            series.append((formats.date_as_millis(first_dimension_value),
                           formats.metric_value(y)))

        return series

    def _render_tooltip(self, metric):
        return {
            "valuePrefix": metric.prefix,
            "valueSuffix": metric.suffix,
            "valueDecimals": metric.precision,
        }
