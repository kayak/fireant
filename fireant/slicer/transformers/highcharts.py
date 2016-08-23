# coding: utf-8
import numpy as np
import pandas as pd

from .base import Transformer, TransformationException


def _format_data_point(value):
    if isinstance(value, str):
        return value
    if isinstance(value, pd.Timestamp):
        return int(value.asm8) // int(1e6)
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, np.int64):
        # Cannot serialize np.int64 to json
        return int(value)
    return value


class HighchartsLineTransformer(Transformer):
    """
    Transforms data frames into Highcharts format for several chart types, particularly line or bar charts.
    """

    chart_type = 'line'

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
        dimension_orders = [order
                            for key, dimension in display_schema['dimensions'].items()
                            for order in [key] + ([dimension['label_field']]
                                                  if 'label_field' in dimension
                                                  else [])]

        reordered = data_frame.reorder_levels(data_frame.index.names.index(level)
                                              for level in dimension_orders)
        return reordered

    def _make_series(self, data_frame, dim_ordinal, display_schema, reference=None):
        metrics = list(data_frame.columns.levels[0]
                       if isinstance(data_frame.columns, pd.MultiIndex)
                       else data_frame.columns)

        return [self._make_series_item(idx, item, dim_ordinal, display_schema, metrics, reference)
                for idx, item in data_frame.iteritems()]

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference):
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': self._format_data(item),
            'yAxis': metrics.index(idx[0] if isinstance(idx, tuple) else idx),
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
            unstack_levels = list(self._unstack_levels(list(dimensions.items())[1:], dim_ordinal))
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

        dim_labels = [self._format_dimension_label(dim_ordinal, key, dimension, idx)
                      for key, dimension in list(display_schema['dimensions'].items())[1:]]
        dim_labels = [dim_label  # filter out the NaNs
                      for dim_label in dim_labels
                      if dim_label is not np.nan]

        return (
            '{metric} ({dimensions})'.format(metric=metric_label, dimensions=', '.join(map(str, dim_labels)))
            if dim_labels else
            metric_label
        )

    @staticmethod
    def _format_dimension_label(dim_ordinal, key, dimension, idx):
        if 'label_field' in dimension:
            label_field = dimension['label_field']
            return idx[dim_ordinal[label_field]]

        dim_label = idx[dim_ordinal[key]]
        if 'label_options' in dimension:
            dim_label = dimension['label_options'].get(dim_label, dim_label)
        return dim_label

    def _format_data(self, column):
        if isinstance(column, float):
            return [_format_data_point(column)]

        return [self._format_point(key, value)
                for key, value in column.iteritems()
                if not np.isnan(value)]

    @staticmethod
    def _format_point(x, y):
        return (_format_data_point(x), _format_data_point(y))

    def _unstack_levels(self, dimensions, dim_ordinal):
        for key, dimension in dimensions:
            yield dim_ordinal[key]

            if 'label_field' in dimension:
                yield dim_ordinal[dimension['label_field']]


class HighchartsColumnTransformer(HighchartsLineTransformer):
    chart_type = 'column'

    def _make_series_item(self, idx, item, dim_ordinal, display_schema, metrics, reference):
        return {
            'name': self._format_label(idx, dim_ordinal, display_schema, reference),
            'data': [_format_data_point(x)
                     for x in item
                     if not np.isnan(x)],
            'yAxis': metrics.index(idx[0] if isinstance(idx, tuple) else idx),
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
        data_frame = data_frame.replace([np.inf, -np.inf], np.nan)

        # Unstack multi-indices
        if 1 < len(dimensions):
            unstack_levels = list(self._unstack_levels(list(dimensions.items())[1:], dim_ordinal))
            data_frame = data_frame.unstack(level=unstack_levels)

        return data_frame

    @staticmethod
    def _make_categories(data_frame, dim_ordinal, display_schema):
        if not display_schema['dimensions']:
            return None

        category_dimension = list(display_schema['dimensions'].values())[0]
        if 'label_options' in category_dimension:
            return [category_dimension['label_options'].get(dim, dim)
                    # Pandas gives both NaN or None in the index depending on whether a level was unstacked
                    if dim and not (isinstance(dim, float) and np.isnan(dim))
                    else 'Totals'
                    for dim in data_frame.index]

        if 'label_field' in category_dimension:
            label_field = category_dimension['label_field']
            return data_frame.index.get_level_values(label_field, ).unique().tolist()


class HighchartsBarTransformer(HighchartsColumnTransformer):
    chart_type = 'bar'
