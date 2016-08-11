# coding: utf-8
import numpy as np
import pandas as pd

from .base import Transformer, TransformationException


class HighchartsTransformer(Transformer):
    """
    Transforms data frames into Highcharts format for several chart types, particularly line or bar charts.
    """

    line = 'line'

    def __init__(self, chart_type=line):
        self.chart_type = chart_type

    def transform(self, data_frame, display_schema):
        self._validate_dimensions(data_frame, display_schema['dimensions'])

        if isinstance(data_frame.index, pd.MultiIndex):
            data_frame = self._reorder_index_levels(data_frame, display_schema)
        has_references = isinstance(data_frame.columns, pd.MultiIndex)

        dim_ordinal = {name: ordinal
                       for ordinal, name in enumerate(data_frame.index.names)}
        data_frame = self._prepare_data_frame(data_frame, dim_ordinal, display_schema['dimensions'])

        if has_references:
            series = sum(
                [self._make_series(data_frame[level], dim_ordinal, display_schema, reference=level or None)
                 for level in data_frame.columns.levels[0]],
                []
            )

        else:
            series = self._make_series(data_frame, dim_ordinal, display_schema)

        result = {
            'chart': {'type': self.chart_type},
            'title': {'text': None},
            'xAxis': self.xaxis_options(data_frame, dim_ordinal, display_schema),
            'yAxis': self.yaxis_options(data_frame, dim_ordinal, display_schema),
            'tooltip': {'shared': True},
            'series': series
        }

        return result

    def xaxis_options(self, data_frame, dim_ordinal, display_schema):
        return {
            'type': 'datetime' if isinstance(data_frame.index, pd.DatetimeIndex) else 'linear'
        }

    def yaxis_options(self, data_frame, dim_ordinal, display_schema):
        if isinstance(data_frame.columns, pd.MultiIndex):
            num_metrics = len(data_frame.columns.levels[0])
        else:
            num_metrics = len(data_frame.columns)

        return [{
            'title': None
        }] * num_metrics

    def _reorder_index_levels(self, data_frame, display_schema):
        dimension_orders = [id_field
                            for d in display_schema['dimensions']
                            for id_field in
                            (d['id_fields'] + (
                                [d['label_field']]
                                if 'label_field' in d
                                else []))]
        reordered = data_frame.reorder_levels(data_frame.index.names.index(level)
                                              for level in dimension_orders)
        return reordered

    def _make_series(self, data_frame, dim_ordinal, display_schema, reference=None):
        # Convert dates to millis
        # if isinstance(data_frame.index, pd.DatetimeIndex):
        #     data_frame.index = data_frame.index.astype(int) // 1e6

        # This value represents how many iterations over data_frame items per yAxis we have.  It's the product of
        # non-metric levels of the data_frame's columns. We want metrics to share the same yAxis for all dimensions.
        yaxis_span = (np.product([len(l) for l in data_frame.columns.levels[1:]])
                      if isinstance(data_frame.columns, pd.MultiIndex)
                      else 1)

        return [self._make_series_item(idx, item, dim_ordinal, display_schema, int(i // yaxis_span), reference)
                for i, (idx, item) in enumerate(data_frame.iteritems())]

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, y_axis, reference):
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'yAxis': y_axis,
            'dashStyle': 'Dot' if reference else 'Solid'
        }

    def _validate_dimensions(self, data_frame, dimensions):
        if not 0 < len(dimensions):
            raise TransformationException('Cannot transform %s chart.  '
                                          'At least one dimension is required.' % self.chart_type)

    @staticmethod
    def _make_categories(data_frame, dim_ordinal, display_schema):
        return None

    def _prepare_data_frame(self, data_frame, dim_ordinal, dimensions):
        # Replaces invalid values and unstacks the data frame for line charts.

        # Force all fields to be float (Safer for highcharts)
        data_frame = data_frame.astype(np.float).replace([np.inf, -np.inf], np.nan)

        # Unstack multi-indices
        if 1 < len(dimensions):
            # We need to unstack all of the dimensions here after the first dimension, which is the first dimension in
            # the dimensions list, not necessarily the one in the dataframe
            unstack_levels = list(self._unstack_levels(dimensions[1:], dim_ordinal))
            data_frame = data_frame.unstack(level=unstack_levels)

        return data_frame

    def _format_label(self, idx, dim_ordinal, display_schema, reference):
        is_multidimensional = isinstance(idx, tuple)
        if is_multidimensional:
            metric_label = display_schema['metrics'].get(idx[0], idx[0])
        else:
            metric_label = display_schema['metrics'].get(idx, idx)

        if reference:
            metric_label += ' {reference}'.format(
                reference=display_schema['references'][reference]
            )

        if not is_multidimensional:
            return metric_label

        dim_labels = [self._format_dimension_label(dim_ordinal, dimension, idx)
                      for dimension in display_schema['dimensions'][1:]]
        dim_labels = [dim_label  # filter out the NaNs
                      for dim_label in dim_labels
                      if dim_label is not np.nan]

        return (
            '{metric} ({dimensions})'.format(metric=metric_label, dimensions=', '.join(map(str, dim_labels)))
            if dim_labels else
            metric_label
        )

    @staticmethod
    def _format_dimension_label(dim_ordinal, dimension, idx):
        if 'label_field' in dimension:
            label_field = dimension['label_field']
            return idx[dim_ordinal[label_field]]

        id_field = dimension['id_fields'][0]
        dim_label = idx[dim_ordinal[id_field]]
        if 'label_options' in dimension:
            dim_label = dimension['label_options'].get(dim_label, dim_label)
        return dim_label

    def _format_data(self, column):
        if isinstance(column, float):
            return [column]

        return [self._format_point(key, value)
                for key, value in column.iteritems()
                if not np.isnan(value)]

    @staticmethod
    def _format_point(x, y):
        # return {'x': x, 'y': y}
        return (
            int(x.asm8) // int(1e6) if isinstance(x, pd.Timestamp) else x,
            # FIXME make this configurable
            round(y, 2)
        )

    def _unstack_levels(self, dimensions, dim_ordinal):
        for dimension in dimensions:
            for id_field in dimension['id_fields']:
                yield dim_ordinal[id_field]

            if 'label_field' in dimension:
                yield dim_ordinal[dimension['label_field']]


class HighchartsColumnTransformer(HighchartsTransformer):
    column = 'column'
    bar = 'bar'

    def __init__(self, chart_type=column):
        super(HighchartsColumnTransformer, self).__init__(chart_type)

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, y_axis, reference):
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'yAxis': y_axis
        }

    def xaxis_options(self, data_frame, dim_ordinal, display_schema):
        result = {'type': 'categorical'}

        categories = self._make_categories(data_frame, dim_ordinal, display_schema)
        if categories is not None:
            result['categories'] = categories

        return result

    def _validate_dimensions(self, data_frame, dimensions):
        if 1 < len(dimensions) and 1 < len(data_frame.columns):
            raise TransformationException('Cannot transform %s chart.  '
                                          'No more than 1 dimension or 2 dimensions '
                                          'with 1 metric are allowed.' % self.chart_type)

    def _prepare_data_frame(self, data_frame, dim_ordinal, dimensions):
        # Replaces invalid values and unstacks the data frame for line charts.

        # Force all fields to be float (Safer for highcharts)
        data_frame = data_frame.astype(np.float).replace([np.inf, -np.inf], np.nan)

        # Unstack multi-indices
        if 1 < len(dimensions):
            unstack_levels = list(self._unstack_levels(dimensions[1:], dim_ordinal))
            data_frame = data_frame.unstack(level=unstack_levels)

        if isinstance(data_frame.index, pd.DatetimeIndex):
            # We need to unstack the second dimension here, which could be one or more levels
            data_frame.index = data_frame.index.astype(str)

        return data_frame

    def _format_data(self, column):
        return list(column)

    @staticmethod
    def _make_categories(data_frame, dim_ordinal, display_schema):
        if not display_schema['dimensions']:
            return None

        category_dimension = display_schema['dimensions'][0]
        if 'label_options' in category_dimension:
            return [category_dimension['label_options'].get(dim, dim)
                    for dim in data_frame.index]

        if 'label_field' in category_dimension:
            label_field = category_dimension['label_field']
            return data_frame.index.get_level_values(label_field).unique().tolist()

        return []
