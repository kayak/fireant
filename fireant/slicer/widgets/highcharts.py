import itertools

import pandas as pd

from fireant import (
    DatetimeDimension,
    formats,
    utils,
)
from .base import TransformableWidget
from .chart_base import (
    ChartWidget,
    ContinuousAxisSeries,
)
from .helpers import (
    dimensional_metric_label,
    extract_display_values,
)
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

SERIES_NEEDING_MARKER = (ChartWidget.LineSeries, ChartWidget.AreaSeries)


class HighCharts(ChartWidget, TransformableWidget):
    # Pagination should be applied to groups of the 0th index level (the x-axis) in order to paginate series
    group_pagination = True

    def __init__(self, title=None, colors=None, x_axis_visible=True, tooltip_visible=True):
        super(HighCharts, self).__init__()
        self.title = title
        self.colors = colors or DEFAULT_COLORS
        self.x_axis_visible = x_axis_visible
        self.tooltip_visible = tooltip_visible

    def __repr__(self):
        return ".".join(["HighCharts()"] + [repr(axis) for axis in self.items])

    def transform(self, data_frame, slicer, dimensions, references):
        """
        - Main entry point -

        Transforms a data frame into HighCharts JSON format.

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

        dimension_display_values = extract_display_values(dimensions, data_frame)
        render_series_label = dimensional_metric_label(dimensions, dimension_display_values)

        # fill NaNs in all index levels besides the 0th
        # the `groupby` function drops all NaN values. Since the 0th dimension is not filled, the totals values for it
        # will get filtered out. This is okay, because there isn't a way to represent the totals across the 0th
        # dimension on a chart.
        if isinstance(data_frame.index, pd.MultiIndex):
            data_frame.update(data_frame.groupby(level=0).fillna(value='Totals'))

        # Group the results by index levels after the 0th, one for each series
        # This will result in a series for every combination of dimension values and each series will contain a data set
        # across the 0th dimension (used for the x-axis)
        series_data_frames = self._group_by_series(data_frame)

        total_num_series = sum([len(axis)
                                for axis in self.items])

        y_axes, series = [], []
        for axis_idx, axis in enumerate(self.items):
            # Tee the colors iterator so we can peek at the next color. The next color in the iterator becomes the
            # axis color. The first series on each axis should match the axis color, and will progress the colors
            # iterator. The next axis color should be the next color in the iterator after the last color used by a
            # series on the current axis in order to get a better variety of color.
            colors, tee_colors = itertools.tee(colors)
            axis_color = next(tee_colors)

            # prepend axes, append series, this keeps everything ordered left-to-right
            y_axes[0:0] = self._render_y_axis(axis_idx,
                                              axis_color if 1 < total_num_series else None,
                                              references)
            is_timeseries = dimensions and isinstance(dimensions[0], DatetimeDimension)

            series += self._render_series(axis,
                                          axis_idx,
                                          axis_color,
                                          colors,
                                          series_data_frames,
                                          render_series_label,
                                          references,
                                          is_timeseries)

        x_axis = self._render_x_axis(data_frame, dimensions, dimension_display_values)

        return {
            "title": {"text": self.title},
            "xAxis": x_axis,
            "yAxis": y_axes,
            'colors': self.colors,
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
            if not isinstance(data_frame.index, pd.MultiIndex) and data_frame.index.name is None \
            else [utils.getdeepattr(dimension_display_values,
                                    (first_level.name, dimension_value),
                                    dimension_value or 'Totals')
                  for dimension_value in first_level]

        return {
            "type": "category",
            "categories": categories,
            "visible": self.x_axis_visible,
        }

    def _group_by_series(self, data_frame):
        if not isinstance(data_frame.index, pd.MultiIndex):
            return [([], data_frame)]

        series = data_frame.index.names[1:]
        return data_frame.groupby(level=series, sort=False)

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

    def _render_series(self, axis, axis_idx, axis_color, colors, series_data_frames, render_series_label,
                       references, is_timeseries=False):
        """
        Renders the series configuration.

        https://api.highcharts.com/highcharts/series

        :param axis:
        :param axis_idx:
        :param axis_color:
        :param colors:
        :param series_data_frames:
        :param render_series_label:
        :param references:
        :param is_timeseries:
        :return:
        """
        hc_series = []
        for series in axis:
            symbols = itertools.cycle(MARKER_SYMBOLS)

            for (dimension_values, group_df), symbol in zip(series_data_frames, symbols):
                if is_timeseries:
                    group_df = group_df.sort_index(level=0)

                dimension_values = utils.wrap_list(dimension_values)

                if isinstance(series, self.PieSeries):
                    # pie charts suck
                    for reference in [None] + references:
                        hc_series.append(self._render_pie_series(series,
                                                                 reference,
                                                                 group_df,
                                                                 render_series_label))
                    continue

                # With a single axis, use different colors for each series
                # With multiple axes, use the same color for the entire axis and only change the dash style
                series_color = next(colors)

                for reference, dash_style in zip([None] + references, itertools.cycle(DASH_STYLES)):
                    metric_key = utils.format_metric_key(reference_key(series.metric, reference))

                    hc_series.append({
                        "type": series.type,

                        "name": render_series_label(dimension_values, series.metric, reference),

                        "data": self._render_data(group_df, metric_key, is_timeseries),

                        "tooltip": self._render_tooltip(series.metric),

                        "yAxis": ("{}_{}".format(axis_idx, reference.key)
                                  if reference is not None and reference.delta
                                  else str(axis_idx)),

                        "marker": ({"symbol": symbol, "fillColor": axis_color or series_color}
                                   if isinstance(series, SERIES_NEEDING_MARKER)
                                   else {}),

                        "stacking": series.stacking,
                    })

                    if isinstance(series, ContinuousAxisSeries):
                        # Set each series in a continuous series to a specific color
                        hc_series[-1]["color"] = series_color
                        hc_series[-1]["dashStyle"] = dash_style

        return hc_series

    def _render_pie_series(self, series, reference, data_frame, render_series_label):
        metric = series.metric
        name = reference_label(metric, reference)
        df_key = utils.format_metric_key(series.metric.key)

        data = []
        for dimension_values, y in data_frame[df_key].sort_values(ascending=False).iteritems():
            data.append({
                "name": render_series_label(dimension_values) if dimension_values else name,
                "y": formats.metric_value(y),
            })

        return {
            "name": name,
            "type": series.type,
            "data": data,
            'tooltip': {
                'pointFormat': '<span style="color:{point.color}">\u25CF</span> {series.name}: '
                               '<b>{point.y} ({point.percentage:.1f}%)</b><br/>',
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
