from functools import partial
from typing import Iterable

import pandas as pd

from fireant import formats
from fireant.dataset.fields import Field
from fireant.utils import (
    alias_selector,
    wrap_list,
)
from .base import (
    ReferenceItem,
    TransformableWidget,
)

HARD_MAX_COLUMNS = 24


class Pandas(TransformableWidget):
    def __init__(self, metric: Field, *metrics: Iterable[Field],
                 pivot=(), transpose=False, sort=None, ascending=None, max_columns=None):
        super(Pandas, self).__init__(metric, *metrics)
        self.pivot = pivot
        self.transpose = transpose
        self.sort = sort
        self.ascending = ascending
        self.max_columns = min(max_columns, HARD_MAX_COLUMNS) \
            if max_columns is not None \
            else HARD_MAX_COLUMNS

    def transform(self, data_frame, slicer, dimensions, references):
        """
        WRITEME

        :param data_frame:
        :param slicer:
        :param dimensions:
        :param references:
        :return:
        """
        result = data_frame.copy()

        items = [item if reference is None else ReferenceItem(item, reference)
                 for reference in [None] + references
                 for item in self.items]

        if isinstance(data_frame.index, pd.MultiIndex):
            index_levels = [alias_selector(dimension.alias)
                            for dimension in dimensions]
            result = result.reorder_levels(index_levels)

        result = result[[alias_selector(item.alias)
                         for item in items]]

        if dimensions:
            result.index.names = [dimension.label or dimension.alias
                                  for dimension in dimensions]

        result.columns = pd.Index([item.label
                                   for item in items],
                                  name='Metrics')

        pivot_dimensions = [dimension.label or dimension.alias for dimension in self.pivot]
        pivot_df = self.pivot_data_frame(result, pivot_dimensions, self.transpose)

        return self.add_formatting(dimensions, items, pivot_df).fillna(value=formats.BLANK_VALUE)

    def pivot_data_frame(self, data_frame, pivot=(), transpose=False):
        """
        Pivot and transpose the data frame. Dimensions including in the `pivot` arg will be unshifted to columns. If
        `transpose` is True the data frame will be transposed. If there is only index level in the data frame (ie. one
        dimension), and that dimension is pivoted, then the data frame will just be transposed. If there is a single
        metric in the data frame and at least one dimension pivoted, the metrics column level will be dropped for
        simplicity.

        :param data_frame:
            The result set data frame
        :param pivot:
            A list of index aliases for `data_frame` of levels to shift
        :param transpose:
            A boolean true or false whether to transpose the data frame.
        :return:
            The shifted/transposed data frame
        """
        not_transforming_df = not (pivot or transpose)
        pivot_and_transpose_cancel_out = transpose and len(pivot) == len(data_frame.index.names)
        if not_transforming_df or pivot_and_transpose_cancel_out:
            return self.sort_data_frame(data_frame)

        # NOTE: Don't pivot a single dimension data frame. This turns the data frame into a series and pivots the
        # metrics anyway. Instead, transpose the data frame.
        should_transpose_instead_of_pivot = len(pivot) == len(data_frame.index.names)

        if pivot and not should_transpose_instead_of_pivot:
            data_frame = data_frame.unstack(level=pivot)

        if transpose or should_transpose_instead_of_pivot:
            data_frame = data_frame.transpose()

        # If there are more than one column levels and the last level is a single metric, drop the level
        if isinstance(data_frame.columns, pd.MultiIndex) and 1 == len(data_frame.columns.levels[0]):
            data_frame.name = data_frame.columns.levels[0][0]  # capture the name of the metrics column
            data_frame.columns = data_frame.columns.droplevel(0)  # drop the metrics level

        return self.sort_data_frame(data_frame)

    def sort_data_frame(self, data_frame):
        if not self.sort or len(data_frame) == 1:
            # If there are no sort arguments or the data frame is a single row, then no need to sort
            return data_frame

        # reset the index so all columns can be sorted together
        index_names = data_frame.index.names
        unsorted = data_frame.reset_index()
        column_names = list(unsorted.columns)

        ascending = self.ascending \
            if self.ascending is not None \
            else True

        sort = wrap_list(self.sort)
        sort_columns = [column_names[column]
                        for column in sort
                        if column < len(column_names)]

        if not sort_columns:
            return data_frame

        # ignore additional values for ascending if they do not align lengthwise with sort_columns
        # Default to the first value or None
        if isinstance(ascending, list) and len(ascending) != len(sort_columns):
            ascending = ascending[0] \
                if len(ascending) > 0 \
                else None

        return unsorted \
            .sort_values(sort_columns, ascending=ascending) \
            .set_index(index_names)

    def add_formatting(self, dimensions, items, pivot_df):
        format_df = pivot_df.copy()

        def _get_f_display(item):
            return partial(formats.display_value, field=item)

        if self.transpose or not self.transpose and len(dimensions) == len(self.pivot) > 0:
            for item in items:
                f_display = _get_f_display(item)
                format_df.loc[items[0].label] = format_df.loc[items[0].label].apply(f_display)

            return format_df

        if self.pivot and len(items) == 1:
            f_display = _get_f_display(items[0])
            format_df = format_df.applymap(f_display)
            return format_df

        for item in items:
            f_display = _get_f_display(item)
            format_df[item.label] = format_df[item.label].apply(f_display)

        return format_df
