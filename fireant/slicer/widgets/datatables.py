import itertools

import pandas as pd

from fireant import (
    ContinuousDimension,
    utils,
)
from . import formats
from .base import Widget


def _format_dimension_cell(dimension_value, display_values):
    dimension_cell = {'value': formats.safe(dimension_value)}

    if display_values is not None:
        dimension_cell['display'] = display_values.get(dimension_value, dimension_value)

    return dimension_cell


def _format_dimensional_metric_cell(row_data, metric):
    level = {}
    for key, next_row in row_data.groupby(level=-1):
        next_row.reset_index(level=-1, drop=True, inplace=True)

        safe_key = formats.safe(key)

        level[safe_key] = _format_dimensional_metric_cell(next_row, metric) \
            if isinstance(next_row.index, pd.MultiIndex) \
            else _format_metric_cell(next_row[metric.key], metric)

    return level


def _format_metric_cell(value, metric):
    raw_value = formats.safe(value)
    return {
        'value': raw_value,
        'display': formats.display(raw_value,
                                   prefix=metric.prefix,
                                   suffix=metric.suffix,
                                   precision=metric.precision)
        if raw_value is not None
        else None
    }


HARD_MAX_COLUMNS = 24


class DataTablesJS(Widget):
    def __init__(self, metrics=(), pivot=False, max_columns=None):
        super(DataTablesJS, self).__init__(metrics)
        self.pivot = pivot
        self.max_columns = min(max_columns, HARD_MAX_COLUMNS) \
            if max_columns is not None \
            else HARD_MAX_COLUMNS

    def transform(self, data_frame, slicer):
        """

        :param data_frame:
        :param slicer:
        :return:
        """
        dimension_keys = list(filter(None, data_frame.index.names))
        dimensions = [getattr(slicer.dimensions, key)
                      for key in dimension_keys]
        dimension_display_values = self._dimension_display_values(dimensions, data_frame)

        metric_keys = [metric.key for metric in self.metrics]
        data_frame = data_frame[metric_keys]

        pivot_index_to_columns = self.pivot and isinstance(data_frame.index, pd.MultiIndex)
        if pivot_index_to_columns:
            levels = list(range(1, len(dimensions)))
            data_frame = data_frame \
                .unstack(level=levels) \
                .fillna(value=0)

            columns = self._dimension_columns(dimensions[:1]) \
                      + self._metric_columns_pivoted(data_frame.columns, dimension_display_values)

        else:
            columns = self._dimension_columns(dimensions) + self._metric_columns()

        data = [self._data_row(dimensions, dimension_values, dimension_display_values, row_data)
                for dimension_values, row_data in data_frame.iterrows()]

        return dict(columns=columns, data=data)

    def _dimension_display_values(self, dimensions, data_frame):
        dv_by_dimension = {}

        for dimension in dimensions:
            dkey = dimension.key
            if hasattr(dimension, 'display_values'):
                dv_by_dimension[dkey] = dimension.display_values
            elif hasattr(dimension, 'display_key'):
                dv_by_dimension[dkey] = data_frame[dimension.display_key].groupby(level=dkey).first()

        return dv_by_dimension

    def _dimension_columns(self, dimensions):
        """

        :param data_frame:
        :param slicer:
        :return:
        """
        columns = []
        for dimension in dimensions:
            column = dict(title=dimension.label or dimension.key,
                          data=dimension.key,
                          render=dict(_='value'))

            if not isinstance(dimension, ContinuousDimension):
                column['render']['display'] = 'display'

            columns.append(column)

        return columns

    def _metric_columns(self):
        """

        :return:
        """
        columns = []
        for metric in self.metrics:
            columns.append(dict(title=metric.label or metric.key,
                                data=metric.key,
                                render=dict(_='value', display='display')))
        return columns

    def _metric_columns_pivoted(self, df_columns, display_values):
        """

        :param index_levels:
        :param display_values:
        :return:
        """
        columns = []
        for metric in self.metrics:
            dimension_keys = df_columns.names[1:]
            for level_values in itertools.product(*map(list, df_columns.levels[1:])):
                level_display_values = [utils.deep_get(display_values, [key, raw_value], raw_value)
                                        for key, raw_value in zip(dimension_keys, level_values)]

                columns.append(dict(title="{metric} ({dimensions})".format(metric=metric.label or metric.key,
                                                                           dimensions=", ".join(level_display_values)),
                                    data='.'.join([metric.key] + [str(x) for x in level_values]),
                                    render=dict(_='value', display='display')))

        return columns

    def _data_row(self, dimensions, dimension_values, dimension_display_values, row_data):
        row = {}

        for dimension, dimension_value in zip(dimensions, utils.wrap_list(dimension_values)):
            row[dimension.key] = _format_dimension_cell(dimension_value, dimension_display_values.get(dimension.key))

        for metric in self.metrics:
            row[metric.key] = _format_dimensional_metric_cell(row_data, metric) \
                if isinstance(row_data.index, pd.MultiIndex) \
                else _format_metric_cell(row_data[metric.key], metric)

        return row
