# coding: utf-8
import numpy as np
import pandas as pd

from fireant.slicer.operations import Totals
from fireant import settings, utils

from .base import Transformer, TransformationException

COLORS = {
    'kayak': ['#FF690F', '#1A1A1A', '#3083F0', '#00B86B', '#D10244', '#FFDBE5', '#7B4F4B', '#B903AA', '#B05B6F'],
    'dark-blue': ["#DDDF0D", "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE", "#FF0066", "#EEAAEE",
                  "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE"],
    'dark-green': ["#DDDF0D", "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE", "#FF0066", "#EEAAEE",
                   "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE"],
    'dark-unica': ["#2B908F", "#90EE7E", "#F45B5B", "#7798BF", "#AAEEEE", "#FF0066", "#EEAAEE",
                   "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE"],
    'gray': ["#DDDF0D", "#7798BF", "#55BF3B", "#DF5353", "#AAEEEE", "#FF0066", "#EEAAEE",
             "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE"],
    'grid-light': ["#7CB5EC", "#F7A35C", "#90EE7E", "#7798BF", "#AAEEEE", "#FF0066", "#EEAAEE",
                   "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE"],
    'grid': ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],
    'sand-signika': ["#F45B5B", "#8085E9", "#8D4654", "#7798BF", "#AAEEEE", "#FF0066", "#EEAAEE",
                     "#55BF3B", "#DF5353", "#7798BF", "#AAEEEE"],
    'skies': ["#514F78", "#42A07B", "#9B5E4A", "#72727F", "#1F949A", "#82914E", "#86777F", "#42A07B"],
}


def _format_data_point(value):
    if isinstance(value, pd.Timestamp):
        return int(value.asm8) // int(1e6)
    if value is None or (isinstance(value, (float, int)) and np.isnan(value)):
        return None
    if isinstance(value, np.int64):
        # Cannot serialize np.int64 to json
        return int(value)
    return value


MISSING_CONT_DIM_MESSAGE = ('Highcharts line charts require a continuous dimension as the first '
                            'dimension.  Please add a continuous dimension from your Slicer to '
                            'your request.')


class HighchartsLineTransformer(Transformer):
    """
    Transforms data frames into Highcharts format for several chart types, particularly line or bar charts.
    """

    chart_type = 'line'

    def prevalidate_request(self, slicer, metrics, dimensions,
                            metric_filters, dimension_filters,
                            references, operations):
        if not dimensions or not slicer.dimensions:
            raise TransformationException(MISSING_CONT_DIM_MESSAGE)

        from fireant.slicer import ContinuousDimension
        dimension0 = slicer.dimensions[dimensions[0]]
        if not isinstance(dimension0, ContinuousDimension):
            raise TransformationException(MISSING_CONT_DIM_MESSAGE)

    def transform(self, dataframe, display_schema):
        has_references = isinstance(dataframe.columns, pd.MultiIndex)

        dim_ordinal = {name: ordinal
                       for ordinal, name in enumerate(dataframe.index.names)}
        dataframe = self._prepare_dataframe(dataframe, dim_ordinal, display_schema['dimensions'])

        if has_references:
            series = sum(
                [self._make_series(dataframe[level], dim_ordinal, display_schema, reference=level or None)
                 for level in dataframe.columns.levels[0]],
                []
            )

        else:
            series = self._make_series(dataframe, dim_ordinal, display_schema)

        result = {
            'chart': {'type': self.chart_type, 'zoomType': 'x'},
            'title': {'text': None},
            'xAxis': self.xaxis_options(dataframe, dim_ordinal, display_schema),
            'yAxis': self.yaxis_options(dataframe, dim_ordinal, display_schema),
            'tooltip': {'shared': True},
            'series': series
        }

        return result

    def xaxis_options(self, dataframe, dim_ordinal, display_schema):
        return {
            'type': 'datetime' if isinstance(dataframe.index, pd.DatetimeIndex) else 'linear'
        }

    def yaxis_options(self, dataframe, dim_ordinal, display_schema):
        axes = {metric_schema.get('axis')
                for metric_schema in display_schema['metrics'].values()}
        return [{'title': None}] * len(axes)

    def _make_series(self, dataframe, dim_ordinal, display_schema, reference=None):
        metrics = list(dataframe.columns.levels[0]
                       if isinstance(dataframe.columns, pd.MultiIndex)
                       else dataframe.columns)

        color = COLORS.get(settings.highcharts_colors, 'grid')
        n_colors = len(color)

        return [self._make_series_item(idx, item, dim_ordinal, display_schema, metrics, reference, color[i % n_colors])
                for i, (idx, item) in enumerate(dataframe.iteritems())]

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        metric_key = utils.slice_first(idx)
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'yAxis': display_schema['metrics'][metric_key].get('axis', 0),
            'color': color,
            'dashStyle': 'Dot' if reference else 'Solid'
        }

    @staticmethod
    def _make_categories(dataframe, dim_ordinal, display_schema):
        return None

    def _format_tooltip(self, metric_schema):
        tooltip = {}

        if 'precision' in metric_schema:
            tooltip['valueDecimals'] = metric_schema['precision']
        if 'prefix' in metric_schema:
            tooltip['valuePrefix'] = metric_schema['prefix']
        if 'suffix' in metric_schema:
            tooltip['valueSuffix'] = metric_schema['suffix']

        return tooltip

    def _prepare_dataframe(self, dataframe, dim_ordinal, dimensions):
        # Replaces invalid values and unstacks the data frame for line charts.

        # Force all fields to be float (Safer for highcharts)
        dataframe = dataframe.astype(np.float).replace([np.inf, -np.inf], np.nan)

        # Unstack multi-indices
        if 1 < len(dimensions):
            # We need to unstack all of the dimensions here after the first dimension, which is the first dimension in
            # the dimensions list, not necessarily the one in the dataframe
            unstack_levels = list(self._unstack_levels(list(dimensions.items())[1:], dim_ordinal))
            dataframe = dataframe.unstack(level=unstack_levels)

        return dataframe

    def _format_label(self, idx, dim_ordinal, display_schema, reference):
        is_multidimensional = isinstance(idx, tuple)
        if is_multidimensional:
            metric = display_schema['metrics'].get(idx[0], idx[0])
        else:
            metric = display_schema['metrics'].get(idx, idx)

        metric_label = metric['label']
        if reference:
            metric_label += ' {reference}'.format(
                reference=display_schema['references'][reference]
            )

        if not is_multidimensional:
            return metric_label

        dimension_labels = [self._format_dimension_display(dim_ordinal, key, dimension, idx)
                            for key, dimension in list(display_schema['dimensions'].items())[1:]]
        dimension_labels = [dimension_label  # filter out the Totals
                            for dimension_label in dimension_labels
                            if dimension_label is not Totals.label]

        return (
            '{metric} ({dimensions})'.format(
                metric=metric_label,
                dimensions=', '.join(map(str, dimension_labels))
            )
            if dimension_labels
            else metric_label
        )

    @staticmethod
    def _format_dimension_display(dim_ordinal, key, dimension, idx):
        if 'display_field' in dimension:
            display_field = dimension['display_field']
            return idx[dim_ordinal[display_field]]

        dimension_value = idx[dim_ordinal[key]]
        if 'display_options' in dimension:
            dimension_value = dimension['display_options'].get(dimension_value, dimension_value)
        return dimension_value

    def _format_data(self, column):
        if isinstance(column, float):
            return [_format_data_point(column)]

        return [self._format_point(key, value)
                for key, value in column.iteritems()
                if not (isinstance(value, (float, int)) and np.isnan(value))]

    @staticmethod
    def _format_point(x, y):
        return (_format_data_point(x), _format_data_point(y))

    def _unstack_levels(self, dimensions, dim_ordinal):
        for key, dimension in dimensions:
            yield dim_ordinal[key]

            if 'display_field' in dimension:
                yield dim_ordinal[dimension['display_field']]


class HighchartsColumnTransformer(HighchartsLineTransformer):
    chart_type = 'column'

    def prevalidate_request(self, slicer, metrics, dimensions,
                            metric_filters, dimension_filters,
                            references, operations):
        if dimensions and 2 < len(dimensions):
            # Too many dimensions
            raise TransformationException('Highcharts bar and column charts support at a maximum two dimensions.  '
                                          'Request included %d dimensions.' % len(dimensions))

        if dimensions and 2 == len(dimensions) and metrics and 1 < len(metrics):
            # Too many metrics
            raise TransformationException('Highcharts bar and column charts support at a maximum one metric with '
                                          'two dimensions.  Please remove some dimensions or metrics.  '
                                          'Request included %d metrics and %d dimensions.' % (len(metrics),
                                                                                              len(dimensions)))

    def yaxis_options(self, dataframe, dim_ordinal, display_schema):
        return [{'title': None }] * len(display_schema['metrics'])

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        metric_key = utils.slice_first(idx)
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': [_format_data_point(x)
                     for x in item
                     if not (isinstance(x, (float, int)) and np.isnan(x))],
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'yAxis': metrics.index(metric_key),
            'color': color,
        }

    def xaxis_options(self, dataframe, dim_ordinal, display_schema):
        result = {'type': 'categorical'}

        categories = self._make_categories(dataframe, dim_ordinal, display_schema)
        if categories is not None:
            result['categories'] = categories

        return result

    def _validate_dimensions(self, dataframe, dimensions):
        if 1 < len(dimensions) and 1 < len(dataframe.columns):
            raise TransformationException('Cannot transform %s chart.  '
                                          'No more than 1 dimension or 2 dimensions '
                                          'with 1 metric are allowed.' % self.chart_type)

    def _prepare_dataframe(self, dataframe, dim_ordinal, dimensions):
        # Replaces invalid values and unstacks the data frame for line charts.

        # Force all fields to be float (Safer for highcharts)
        dataframe = dataframe.replace([np.inf, -np.inf], np.nan)

        # Unstack multi-indices
        if 1 < len(dimensions):
            unstack_levels = list(self._unstack_levels(list(dimensions.items())[1:], dim_ordinal))
            dataframe = dataframe.unstack(level=unstack_levels)

        return dataframe

    @staticmethod
    def _make_categories(dataframe, dim_ordinal, display_schema):
        if not display_schema['dimensions']:
            return None

        category_dimension = list(display_schema['dimensions'].values())[0]

        if 'display_field' in category_dimension:
            display_field = category_dimension['display_field']
            return dataframe.index.get_level_values(display_field).tolist()

        if isinstance(dataframe.index, pd.DatetimeIndex):
            if any(value.time() for value in dataframe.index):
                return [value.isoformat() for value in dataframe.index]
            return [value.strftime("%y-%m-%d") for value in dataframe.index]

        display_options = category_dimension.get('category_dimension', {})
        return [display_options.get(value, value) for value in dataframe.index]


class HighchartsBarTransformer(HighchartsColumnTransformer):
    chart_type = 'bar'
