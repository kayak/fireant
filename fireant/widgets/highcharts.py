import itertools
from datetime import timedelta

import pandas as pd
from fireant import (
    DataType,
    formats,
    utils,
)
from fireant.dataset.totals import TOTALS_MARKERS
from fireant.reference_helpers import (
    reference_alias,
    reference_label,
    reference_prefix,
    reference_suffix,
)

from .base import TransformableWidget
from .chart_base import (
    ChartWidget,
    ContinuousAxisSeries,
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
TS_UPPER_BOUND = pd.Timestamp.max - timedelta(seconds=1)


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

        is_timeseries = dimensions and dimensions[0].data_type == DataType.date

        dimension_map = {dimension_alias: dimension
                         for dimension_alias, dimension in zip(data_frame.index.names, dimensions)}
        num_dimensions = len(dimension_map)
        dimension_fields = [dimension_map[field_alias]
                            for field_alias in data_frame.index.names[:num_dimensions]]

        # Timestamp.max is used as a marker for rolled up dimensions (totals). Filter out the totals value for the
        # dimension used for the x-axis
        if is_timeseries and len(data_frame) > 0:
            data_frame = self._remove_date_totals(data_frame)

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

            series += self._render_series(axis,
                                          axis_idx,
                                          axis_color,
                                          colors,
                                          data_frame,
                                          series_data_frames,
                                          dimension_fields,
                                          references,
                                          is_timeseries)

        x_axis = self._render_x_axis(data_frame, dimensions, slicer.fields)

        return {
            "title": {"text": self.title},
            "xAxis": x_axis,
            "yAxis": y_axes,
            'colors': self.colors,
            "series": series,
            "tooltip": {"shared": True, "useHTML": True, "enabled": self.tooltip_visible},
            "legend": {"useHTML": True},
        }

    def _render_x_axis(self, data_frame, dimensions, fields):
        """
        Renders the xAxis configuration.

        https://api.highcharts.com/highcharts/xAxis

        :param data_frame:
        :param dimensions:
        :return:
        """
        is_mi = isinstance(data_frame.index, pd.MultiIndex)
        first_level = data_frame.index.levels[0] \
            if is_mi \
            else data_frame.index

        is_timeseries = dimensions and dimensions[0].data_type == DataType.date
        if is_timeseries:
            return {
                "type": "datetime",
                "visible": self.x_axis_visible,
            }

        categories = ["All"]
        if first_level.name is not None:
            dimension_alias = utils.alias_for_alias_selector(first_level.name)
            dimension = fields[dimension_alias]
            categories = [formats.display_value(category, dimension) or category
                          for category in first_level]

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
            "id": "{}_{}".format(axis_idx, reference.alias),
            "title": {"text": reference.label},
            "opposite": True,
            "labels": {"style": {"color": color}},
            "visible": axis.y_axis_visible,
        }
            for reference in references
            if reference.delta]

        return y_axes

    def _render_series(self, axis, axis_idx, axis_color, colors, data_frame, series_data_frames, dimension_fields,
                       references, is_timeseries=False):
        """
        Renders the series configuration.

        https://api.highcharts.com/highcharts/series

        :param axis:
        :param axis_idx:
        :param axis_color:
        :param colors:
        :param series_data_frames:
        :param dimension_fields:
        :param references:
        :param is_timeseries:
        :return:
        """

        hc_series = []
        for series in axis:
            # Pie Charts, do not break data out into groups, render all data in one pie chart series
            if isinstance(series, self.PieSeries):
                # Pie charts do not group the data up by series so render them separately and then go to the next series
                for reference in [None] + references:
                    hc_series.append(self._render_pie_series(series.metric,
                                                             reference,
                                                             data_frame,
                                                             dimension_fields))
                continue

            # For other series types, create a highcharts series for each group (combination of dimension values)

            symbols = itertools.cycle(MARKER_SYMBOLS)
            for (dimension_values, group_df), symbol in zip(series_data_frames, symbols):
                dimension_values = utils.wrap_list(dimension_values)
                dimension_label = self._format_dimension_values(dimension_fields[1:], dimension_values)

                hc_series += self._render_highcharts_series(series,
                                                            group_df,
                                                            references,
                                                            dimension_label,
                                                            is_timeseries,
                                                            symbol,
                                                            axis_idx,
                                                            axis_color,
                                                            next(colors))

        return hc_series

    def _render_highcharts_series(self, series, series_df, references, dimension_label, is_timeseries, symbol, axis_idx,
                                  axis_color, series_color):
        """

        Note on colors:
        - With a single axis, use different colors for each series
        - With multiple axes, use the same color for the entire axis and only change the dash style

        :param series:
        :param series_df:
        :param references:
        :param dimension_label:
        :param is_timeseries:
        :param symbol:
        :param axis_idx:
        :param axis_color:
        :param series_color:
        :return:
        """
        if is_timeseries:
            series_df = series_df.sort_index(level=0)

        results = []
        for reference, dash_style in zip([None] + references, itertools.cycle(DASH_STYLES)):
            field_alias = utils.alias_selector(reference_alias(series.metric, reference))
            metric_label = reference_label(series.metric, reference)

            hc_series = ({
                "type": series.type,
                "name": '{} ({})'.format(metric_label, dimension_label) if dimension_label else metric_label,

                "data": (
                    self._render_timeseries_data(series_df, field_alias, series.metric)
                    if is_timeseries
                    else self._render_category_data(series_df, field_alias, series.metric)
                ),

                "tooltip": self._render_tooltip(series.metric, reference),

                "yAxis": ("{}_{}".format(axis_idx, reference.alias)
                          if reference is not None and reference.delta
                          else str(axis_idx)),

                "marker": ({"symbol": symbol, "fillColor": axis_color or series_color}
                           if isinstance(series, SERIES_NEEDING_MARKER)
                           else {}),

                "stacking": series.stacking,
            })

            if isinstance(series, ContinuousAxisSeries):
                # Set each series in a continuous series to a specific color
                hc_series["color"] = series_color
                hc_series["dashStyle"] = dash_style

            results.append(hc_series)

        return results

    @staticmethod
    def _render_category_data(group_df, field_alias, metric):
        categories = list(group_df.index.levels[0]) \
            if isinstance(group_df.index, pd.MultiIndex) \
            else list(group_df.index)

        series = []
        for labels, y in group_df[field_alias].iteritems():
            label = labels[0] if isinstance(labels, tuple) else labels

            series.append({
                'x': categories.index(label),
                'y': formats.raw_value(y, metric)
            })

        return series

    @staticmethod
    def _render_timeseries_data(group_df, metric_alias, metric):
        series = []
        for dimension_values, y in group_df[metric_alias].iteritems():
            first_dimension_value = utils.wrap_list(dimension_values)[0]

            # Ignore empty result sets where the only row is totals
            if first_dimension_value in TOTALS_MARKERS:
                continue

            if pd.isnull(first_dimension_value):
                # Ignore totals on the x-axis.
                continue

            series.append((formats.date_as_millis(first_dimension_value),
                           formats.raw_value(y, metric)))
        return series

    def _render_tooltip(self, metric, reference):
        return {
            "valuePrefix": reference_prefix(metric, reference),
            "valueSuffix": reference_suffix(metric, reference),
            "valueDecimals": metric.precision,
        }

    def _render_pie_series(self, metric, reference, data_frame, dimension_fields):
        metric_alias = utils.alias_selector(metric.alias)

        data = []
        for dimension_values, y in data_frame[metric_alias].iteritems():
            dimension_values = utils.wrap_list(dimension_values)
            name = self._format_dimension_values(dimension_fields, dimension_values)

            data.append({
                "name": name or metric.label,
                "y": formats.raw_value(y, metric),
            })

        return {
            "name": reference_label(metric, reference),
            "type": 'pie',
            "data": data,
            'tooltip': {
                'pointFormat': '<span style="color:{point.color}">\u25CF</span> {series.name}: '
                               '<b>{point.y} ({point.percentage:.1f}%)</b><br/>',
                'valueDecimals': metric.precision,
                'valuePrefix': reference_prefix(metric, reference),
                'valueSuffix': reference_suffix(metric, reference),
            },
        }

    @staticmethod
    def _remove_date_totals(data_frame):
        """
        This function filters the totals value for the date/time dimension from the result set. There is no way to
        represent this value on a chart so it is just removed.

        :param data_frame:
        :return:
        """
        if isinstance(data_frame.index, pd.MultiIndex):
            index_slice = data_frame.index.get_level_values(0) < TS_UPPER_BOUND
            return data_frame.loc[index_slice, :]

        if isinstance(data_frame.index, pd.DatetimeIndex):
            return data_frame[data_frame.index < TS_UPPER_BOUND]

        return data_frame

    @staticmethod
    def _group_by_series(data_frame):
        if len(data_frame) == 0 or not isinstance(data_frame.index, pd.MultiIndex):
            return [([], data_frame)]

        levels = data_frame.index.names[1:]
        return data_frame.groupby(level=levels, sort=False)

    @staticmethod
    def _format_dimension_values(dimension_fields, dimension_values):
        return ', '.join(str.strip(formats.display_value(value, dimension_field) or str(value))
                         for value, dimension_field in zip(dimension_values, dimension_fields))
