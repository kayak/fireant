import itertools

import pandas as pd
from fireant import (
    ContinuousDimension,
    utils,
)

from . import formats
from .base import MetricsWidget
from .helpers import (
    dimensional_metric_label,
    extract_display_values,
    reference_key,
    reference_label,
)


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


class DataTablesJS(MetricsWidget):
    def __init__(self, metrics=(), pivot=False, max_columns=None):
        super(DataTablesJS, self).__init__(metrics)
        self.pivot = pivot
        self.max_columns = min(max_columns, HARD_MAX_COLUMNS) \
            if max_columns is not None \
            else HARD_MAX_COLUMNS

    def transform(self, data_frame, slicer, dimensions):
        """

        :param data_frame:
        :param slicer:
        :param dimensions:
        :return:
        """
        dimension_display_values = extract_display_values(dimensions, data_frame)

        references = [reference
                      for dimension in dimensions
                      for reference in getattr(dimension, 'references', ())]

        metric_keys = [reference_key(metric, reference)
                       for metric in self.metrics
                       for reference in [None] + references]
        data_frame = data_frame[metric_keys]

        pivot_index_to_columns = self.pivot and isinstance(data_frame.index, pd.MultiIndex)
        if pivot_index_to_columns:
            levels = list(range(1, len(dimensions)))
            data_frame = data_frame \
                .unstack(level=levels) \
                .fillna(value=0)

            dimension_columns = self._dimension_columns(dimensions[:1])

            render_column_label = dimensional_metric_label(dimensions, dimension_display_values)
            metric_columns = self._metric_columns_pivoted(references,
                                                          data_frame.columns,
                                                          render_column_label)


        else:
            dimension_columns = self._dimension_columns(dimensions)
            metric_columns = self._metric_columns(references)

        columns = (dimension_columns + metric_columns)[:self.max_columns]
        data = [self._data_row(dimensions,
                               dimension_values,
                               dimension_display_values,
                               references,
                               row_data)
                for dimension_values, row_data in data_frame.iterrows()]

        return dict(columns=columns, data=data)

    @staticmethod
    def _dimension_columns(dimensions):
        """

        :param dimensions:
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

    def _metric_columns(self, references):
        """

        :return:
        """
        columns = []
        for metric in self.metrics:
            for reference in [None] + references:
                title = reference_label(metric, reference)
                data = reference_key(metric, reference)

                columns.append(dict(title=title,
                                    data=data,
                                    render=dict(_='value', display='display')))
        return columns

    def _metric_columns_pivoted(self, references, df_columns, render_column_label):
        """

        :param references:
        :param df_columns:
        :param render_column_label:
        :return:
        """
        columns = []
        for metric in self.metrics:
            dimension_value_sets = [list(level)
                                    for level in df_columns.levels[1:]]

            for dimension_values in itertools.product(*dimension_value_sets):
                for reference in [None] + references:
                    key = reference_key(metric, reference)
                    title = render_column_label(metric, reference, dimension_values)
                    data = '.'.join([key] + [str(x) for x in dimension_values])

                    columns.append(dict(title=title,
                                        data=data,
                                        render=dict(_='value', display='display')))

        return columns

    def _data_row(self, dimensions, dimension_values, dimension_display_values, references, row_data):
        """

        :param dimensions:
        :param dimension_values:
        :param dimension_display_values:
        :param row_data:
        :return:
        """
        row = {}

        for dimension, dimension_value in zip(dimensions, utils.wrap_list(dimension_values)):
            row[dimension.key] = _format_dimension_cell(dimension_value, dimension_display_values.get(dimension.key))

        for metric in self.metrics:
            for reference in [None] + references:
                key = reference_key(metric, reference)

                row[key] = _format_dimensional_metric_cell(row_data, metric) \
                    if isinstance(row_data.index, pd.MultiIndex) \
                    else _format_metric_cell(row_data[key], metric)

        return row
