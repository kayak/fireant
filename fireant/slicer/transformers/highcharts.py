# coding: utf-8
import numpy as np
import pandas as pd

from collections import Counter

from fireant import settings, utils
from fireant.slicer.operations import Totals
from .base import Transformer, TransformationException

COLORS = {
    'kayak': ['#FF690F', '#3083F0', '#00B86B', '#D10244', '#FFDBE5', '#7B4F4B', '#B903AA', '#B05B6F'],
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


def _color(i):
    colors = COLORS.get(settings.highcharts_colors, 'grid')
    n_colors = len(colors)

    return colors[i % n_colors]


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
        first_dimension = utils.slice_first(dimensions[0])
        dimension0 = slicer.dimensions[first_dimension]
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
            'plotOptions': {},
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
        metrics_per_axis = Counter((metric['axis'] for metric in display_schema['metrics'].values()))

        metric_idx = 0
        axis_options = {}

        for axes_id, metrics_count in metrics_per_axis.items():
            metric_idx += metrics_count
            axis_options[axes_id] = {
                'opposite': metrics_count > 1, 'color': 'black' if metrics_count > 1 else _color(metric_idx - 1)
            }

        # Axes with just one metric have the same color of their line. References and multiple metrics' axis
        # will default to black.
        axis = [
            {
                'id': axes_id, 'opposite': options['opposite'], 'title': {'text': None},
                'labels': {
                    'style': {
                        'color': options['color']
                    }
                }
            }
            for axes_id, options in axis_options.items()
        ]

        reference_keys = display_schema.get('references', {}).keys()

        # Create only one axes per available modifier (i.e. delta percent, percent and none).
        reference_axes_ids = set(self._reference_axes_id(reference_key) for reference_key in reference_keys)

        for i, axes_id in enumerate(reference_axes_ids):
            axis.append({'id': axes_id, 'title': {'text': 'References' if i == 0 else None}, 'opposite': True})

        return axis


    def _make_series(self, dataframe, dim_ordinal, display_schema, reference=None):
        metrics = list(dataframe.columns.levels[0]
                       if isinstance(dataframe.columns, pd.MultiIndex)
                       else dataframe.columns)

        return [self._make_series_item(idx, item, dim_ordinal, display_schema, metrics, reference, _color(i))
                for i, (idx, item) in enumerate(dataframe.iteritems())]

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        metric_key = utils.slice_first(idx)

        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'yAxis': display_schema['metrics'][metric_key].get('axis', 0)
                     if not reference else self._reference_axes_id(reference),
            'color': color,
            'dashStyle': 'Dot' if reference else 'Solid'
        }

    @staticmethod
    def _reference_axes_id(reference_key):
        if '_' in reference_key:
            # Replace dod, wow, mom and yoy with reference, since each modifier type should have only one axes.
            modifier = '_'.join(reference_key.split('_')[1:])
            return 'reference_{}'.format(modifier)
        return 'reference'

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

        metric_idx = idx[0] if is_multidimensional else idx
        metric = display_schema['metrics'].get(metric_idx, {})
        metric_label = metric.get('label', metric_idx)

        if reference:
            metric_label += ' {}'.format(display_schema['references'][reference])

        if not is_multidimensional:
            return metric_label

        dimension_labels = [self._format_dimension_display(dim_ordinal, key, dimension, idx)
                            for key, dimension in list(display_schema['dimensions'].items())[1:]]

        dimension_labels = [dimension_label  # filter out the Totals
                            for dimension_label in dimension_labels
                            if dimension_label and dimension_label is not Totals.label]

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
        if not isinstance(idx, (list, tuple)):
            idx = [idx]

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
                if not (isinstance(value, (float, int)) and np.isnan(value)) and not pd.isnull(key)]

    @staticmethod
    def _format_point(x, y):
        return (_format_data_point(x), _format_data_point(y))

    def _unstack_levels(self, dimensions, dim_ordinal):
        for key, dimension in dimensions:
            yield dim_ordinal[key]

            if 'display_field' in dimension:
                yield dim_ordinal[dimension['display_field']]


class HighchartsAreaTransformer(HighchartsLineTransformer):
    """
    Transformer for a Highcharts Area chart
    http://www.highcharts.com/demo/area-basic
    """
    chart_type = 'area'

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        """
        Overriding the parent class' _make_series_item to remove the yAxis key as area charts
        only really make sense on a single y axis
        """
        metric_key = utils.slice_first(idx)
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'color': color,
            'dashStyle': 'Dot' if reference else 'Solid'
        }

    def yaxis_options(self, dataframe, dim_ordinal, display_schema):
        return [{'title': None}]


class HighchartsAreaPercentageTransformer(HighchartsAreaTransformer):
    """
    Transformer for a Highcharts Area Percentage chart
    http://www.highcharts.com/demo/area-stacked-percent
    """
    chart_type = 'area'

    def transform(self, dataframe, display_schema):
        config = super(HighchartsAreaPercentageTransformer, self).transform(dataframe, display_schema)
        config['plotOptions'] = {
            'area': {
                'stacking': 'percent',
            }
        }
        return config

    def _format_tooltip(self, metric_schema):
        tooltip = super(HighchartsAreaPercentageTransformer, self)._format_tooltip(metric_schema)
        # Add the percentage to the default tooltip point format
        tooltip['pointFormat'] = '<span style="color:{point.color}">\u25CF</span> {series.name}: ' \
                                 '<b>{point.y} - {point.percentage:.1f}%</b><br/>'
        return tooltip


class HighchartsColumnTransformer(HighchartsLineTransformer):
    """
    Transformer for a Highcharts Column chart
    http://www.highcharts.com/demo/column-basic
    """
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

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        metric_key = utils.slice_first(idx)
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'yAxis': display_schema['metrics'][metric_key].get('axis', 0)
                     if not reference else self._reference_axes_id(reference),
            'color': color
        }

    def xaxis_options(self, dataframe, dim_ordinal, display_schema):
        if isinstance(dataframe.index, pd.DatetimeIndex):
            return {'type': 'datetime'}

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

        display_options = category_dimension.get('category_dimension', {})
        return [display_options.get(value, value) for value in dataframe.index]


class HighchartsStackedColumnTransformer(HighchartsColumnTransformer):
    """
    Transformer for a Highcharts Stacked Column chart
    http://www.highcharts.com/demo/column-stacked
    """

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        metric_key = utils.slice_first(idx)
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'color': color
        }

    def transform(self, dataframe, display_schema):
        result = super(HighchartsStackedColumnTransformer, self).transform(dataframe, display_schema)
        result['plotOptions'] = result.get('plotOptions', {})
        result['plotOptions'].update({
            'column': {
                'stacking': 'normal'
            }
        })

        return result

    def yaxis_options(self, dataframe, dim_ordinal, display_schema):
        return [{'title': None}]


class HighchartsBarTransformer(HighchartsColumnTransformer):
    """
    Transformer for a Highcharts Bar chart
    http://www.highcharts.com/demo/bar-basic
    """
    chart_type = 'bar'


class HighchartsStackedBarTransformer(HighchartsBarTransformer):
    """
    Transformer for a Highcharts Stacked Bar chart
    http://www.highcharts.com/demo/bar-stacked
    """

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference, color='#000'):
        metric_key = utils.slice_first(idx)
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'color': color
        }

    def transform(self, dataframe, display_schema):
        result = super(HighchartsStackedBarTransformer, self).transform(dataframe, display_schema)
        result['plotOptions'] = result.get('plotOptions', {})
        result['plotOptions'].update({
            'series': {
                'stacking': 'normal'
            }
        })

        return result

    def yaxis_options(self, dataframe, dim_ordinal, display_schema):
        return [{'title': None}]


class HighchartsPieTransformer(HighchartsLineTransformer):
    """
    Transformer to create the correct JSON payload for Highcharts Pie Charts
    http://www.highcharts.com/demo/pie-basic
    """
    chart_type = 'pie'

    def prevalidate_request(self, slicer, metrics, dimensions,
                            metric_filters, dimension_filters,
                            references, operations):
        """
        Ensure no references or operations are passed and that there is no more than one enabled metric
        """

        if len(references) > 0 or len(operations) > 0:
            raise TransformationException('References and Operations cannot be used with '
                                          '{} charts'.format(self.chart_type))

        if len(utils.flatten(metrics)) > 1:
            raise TransformationException('Only one metric can be specified when using '
                                          '{} charts'.format(self.chart_type))

    def transform(self, dataframe, display_schema):
        """
        Transform the dataframe into the format Highcharts expects for Pie charts

        :param dataframe: Dataframe containing queried data
        :param display_schema: Dictionary defining how the metrics and dimensions should be displayed
        :return: Dictionary in the required Highcharts Pie chart format ready to be dumped into JSON
        """
        dim_ordinal = {name: ordinal
                       for ordinal, name in enumerate(dataframe.index.names)}
        dataframe = self._prepare_dataframe(dataframe, dim_ordinal, display_schema['dimensions'])
        series = self._make_series(dataframe, dim_ordinal, display_schema)

        # Issues have been seen when showing over 900 data points on a Highcharts pie chart.
        max_data_points = 900
        num_data_points = len(series['data'])
        if num_data_points > max_data_points:
            raise TransformationException('You have reached the maximum number of data points that can be shown '
                                          'on a pie chart. Maximum number of data points: {}. '
                                          'Current number of data points: {}.'.format(max_data_points, num_data_points))

        metric_key = self._get_metric_key(dataframe)

        result = {
            'chart': {'type': self.chart_type},
            'title': {'text': None},
            'tooltip': self._format_tooltip(display_schema['metrics'][metric_key]),
            'series': [series],
            'plotOptions': {
                'pie': {
                    'allowPointSelect': True,  # Allows users to select a piece of pie and it separates out
                    'cursor': 'pointer',
                    'dataLabels': {
                        'enabled': True,
                        'format': '<b>{point.name}</b>: {point.percentage:.1f} %',
                        'style': {
                            'color': COLORS.get(settings.highcharts_colors, 'grid')
                        }
                    }
                }
            },
        }

        return result

    def _prepare_dataframe(self, dataframe, dim_ordinal, dimensions):
        """
        Force all fields to be float (Safer for highcharts)

        :param dataframe: Dataframe containing queried data
        :param dim_ordinal: Dictionary containing dimensions with their associated order
        :param dimensions: OrderedDict of dimensions along with their display options/display field
        :return: Dataframe with float values and without np.inf values
        """
        return dataframe.astype(np.float).replace([np.inf, -np.inf], np.nan)

    @staticmethod
    def _get_metric_key(dataframe):
        """
        Get the metric label (There will only ever be one metric)

        :param dataframe: Dataframe containing queried data
        """
        metrics = list(dataframe.columns.levels[0]
                       if isinstance(dataframe.columns, pd.MultiIndex)
                       else dataframe.columns)
        return metrics[0]

    def _make_series(self, dataframe, dim_ordinal, display_schema, reference=None):
        """
        Create the series. Pie charts only ever have a single series.

        :param dataframe: Dataframe containing queried data
        :param dim_ordinal: Dictionary containing dimensions with their associated order
        :param display_schema: Dictionary defining how the metrics and dimensions should be displayed
        :param reference: Not used - set here to match parent classes' function signature
        :return: Dictonary containing the series name and data list
        """
        metric_key = self._get_metric_key(dataframe)
        return {
            'name': display_schema['metrics'][metric_key].get('label', metric_key),
            'data': [(self._format_label(idx, dim_ordinal, display_schema, reference), _format_data_point(item))
                     for idx, item in dataframe[metric_key].iteritems()]
        }

    def _format_label(self, idx, dim_ordinal, display_schema, reference):
        """
        Create the labels for the pie pieces. If there is more than one dimension,
        these will be shown in the format (dim1, dim2). Otherwise, it will show just the
        individual dimension without parentheses.

        :param idx: Dataframe index
        :param dim_ordinal: Dictionary containing dimensions with their associated order
        :param display_schema: Dictionary defining how the metrics and dimensions should be displayed
        :param reference: Not used - set here to match parent classes' function signature
        :return: Dimension label string
        """
        dimension_labels = [self._format_dimension_display(dim_ordinal, key, dimension, idx)
                            for key, dimension in list(display_schema['dimensions'].items())]

        label = (
            '{dimensions}'.format(
                dimensions=', '.join(map(str, dimension_labels))
            )
            if dimension_labels else ''
        )
        return '({})'.format(label) if len(dimension_labels) > 1 else label
