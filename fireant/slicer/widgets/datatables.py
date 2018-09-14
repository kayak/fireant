import pandas as pd

from fireant import (
    ContinuousDimension,
    Metric,
    UniqueDimension,
    formats,
    utils,
)
from fireant.utils import (
    format_dimension_key,
    format_metric_key,
)
from .base import (
    TransformableWidget,
)
from .helpers import (
    dimensional_metric_label,
    extract_display_values,
)
from ..references import (
    reference_key,
    reference_label,
)


def _render_dimension_cell(dimension_value: str, display_values: dict):
    """
    Renders a table cell in a dimension column.

    :param dimension_value:
        The raw value for the table cell.

    :param display_values:
        The display value mapped from the raw value.

    :return:
        A dict with the keys value and possible display.
    """
    dimension_cell = {'value': formats.dimension_value(dimension_value)}

    if display_values is not None:
        if pd.isnull(dimension_value):
            dimension_cell['display'] = 'Totals'
        else:
            display_value = display_values.get(dimension_value, dimension_value)
            dimension_cell['display'] = formats.dimension_value(display_value)

    return dimension_cell


def _render_dimensional_metric_cell(row_data: pd.Series, metric: Metric):
    """
    Renders a table cell in a metric column for pivoted tables where there are two or more dimensions. This function
    is recursive to traverse multi-dimensional indices.

    :param row_data:
        A series containing the value for the metric and it's index (for the dimension values).

    :param metric:
        A reference to the slicer metric to access the display formatting.

    :return:
        A deep dict in a tree structure with keys matching each dimension level. The top level will have keys matching
        the first level of dimension values, and the next level will contain the next level of dimension values, for as
        many index levels as there are. The last level will contain the return value of `_format_metric_cell`.
    """
    level = {}

    # Group by the last dimension, drop it, and fill the dict with either the raw metric values or the next level of
    # dicts.
    for key, next_row in row_data.groupby(level=1):
        next_row.reset_index(level=1, drop=True, inplace=True)

        df_key = format_metric_key(metric.key)
        level[key] = _render_dimensional_metric_cell(next_row, metric) \
            if isinstance(next_row.index, pd.MultiIndex) \
            else _format_metric_cell(next_row[df_key], metric)

    return level


def _format_metric_cell(value, metric):
    """
    Renders a table cell in a metric column for non-pivoted tables.

    :param value:
        The raw value of the metric.

    :param metric:
        A reference to the slicer metric to access the display formatting.
    :return:
        A dict containing the keys value and display with the raw and display metric values.
    """
    raw_value = formats.metric_value(value)
    return {
        'value': raw_value,
        'display': formats.metric_display(raw_value,
                                          prefix=metric.prefix,
                                          suffix=metric.suffix,
                                          precision=metric.precision)
        if raw_value is not None
        else None
    }


HARD_MAX_COLUMNS = 24


class DataTablesJS(TransformableWidget):
    def __init__(self, metric, *metrics: Metric, pivot=False, max_columns=None):
        super(DataTablesJS, self).__init__(metric, *metrics)
        self.pivot = pivot
        self.max_columns = min(max_columns, HARD_MAX_COLUMNS) \
            if max_columns is not None \
            else HARD_MAX_COLUMNS

    def __repr__(self):
        return '{}({},pivot={})'.format(self.__class__.__name__,
                                        ','.join(str(m) for m in self.items),
                                        self.pivot)

    def transform(self, data_frame, slicer, dimensions, references):
        """
        WRITEME

        :param data_frame:
        :param slicer:
        :param dimensions:
        :return:
        """
        dimension_display_values = extract_display_values(dimensions, data_frame)

        metric_keys = [format_metric_key(reference_key(metric, reference))
                       for metric in self.items
                       for reference in [None] + references]
        data_frame = data_frame[metric_keys]

        pivot_index_to_columns = self.pivot and isinstance(data_frame.index, pd.MultiIndex)
        if pivot_index_to_columns:
            levels = data_frame.index.names[1:]
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
        WRITEME

        :param dimensions:
        :return:
        """
        columns = []
        for dimension in dimensions:
            column = dict(title=dimension.label or dimension.key,
                          data=dimension.key,
                          render=dict(_='value'))

            is_cont_dim = isinstance(dimension, ContinuousDimension)
            is_uni_dim_no_display = isinstance(dimension, UniqueDimension) \
                                    and not dimension.has_display_field

            if not is_cont_dim and not is_uni_dim_no_display:
                column['render']['display'] = 'display'

            columns.append(column)

        return columns

    def _metric_columns(self, references):
        """
        WRITEME

        :return:
        """
        columns = []
        for metric in self.items:
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
        single_metric = 1 == len(self.items)
        for metric in self.items:
            dimension_value_sets = [row[1:]
                                    for row in list(df_columns)]

            for dimension_values in dimension_value_sets:
                for reference in [None] + references:
                    key = reference_key(metric, reference)
                    title = render_column_label(dimension_values, None if single_metric else metric, reference)
                    data = '.'.join([key] + [str(x) for x in dimension_values])

                    columns.append(dict(title=title,
                                        data=data,
                                        render=dict(_='value', display='display')))

        return columns

    def _data_row(self, dimensions, dimension_values, dimension_display_values, references, row_data):
        """
        WRITEME

        :param dimensions:
        :param dimension_values:
        :param dimension_display_values:
        :param row_data:
        :return:
        """
        row = {}

        for dimension, dimension_value in zip(dimensions, utils.wrap_list(dimension_values)):
            df_key = format_dimension_key(dimension.key)
            row[dimension.key] = _render_dimension_cell(dimension_value, dimension_display_values.get(df_key))

        for metric in self.items:
            for reference in [None] + references:
                key = reference_key(metric, reference)
                df_key = format_metric_key(key)

                row[key] = _render_dimensional_metric_cell(row_data, metric) \
                    if isinstance(row_data.index, pd.MultiIndex) \
                    else _format_metric_cell(row_data[df_key], metric)

        return row
