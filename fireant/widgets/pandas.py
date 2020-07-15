import pandas as pd
from functools import partial
from typing import Iterable

from fireant import formats
from fireant.dataset.fields import Field
from fireant.utils import alias_selector, wrap_list
from .base import ReferenceItem, TransformableWidget

HARD_MAX_COLUMNS = 24


class Pandas(TransformableWidget):
    def __init__(
        self,
        metric: Field,
        *metrics: Iterable[Field],
        pivot=(),
        hide=(),
        transpose=False,
        sort=None,
        ascending=None,
        max_columns=None
    ):
        super(Pandas, self).__init__(metric, *metrics)
        self.pivot = pivot
        self.hide = hide
        self.transpose = transpose
        self.sort = sort
        self.ascending = ascending
        self.max_columns = (
            min(max_columns, HARD_MAX_COLUMNS)
            if max_columns is not None
            else HARD_MAX_COLUMNS
        )

    def transform(
        self,
        data_frame,
        dimensions,
        references,
        annotation_frame=None,
        use_raw_values=False,
    ):
        """
        WRITEME

        :param data_frame:
        :param dimensions:
        :param references:
        :param annotation_frame:
            A data frame containing the annotation data.
        :param use_raw_values:
            Don't add prefix or postfix to values.
        """
        result_df = data_frame.copy()

        dimension_aliases = [
            alias_selector(dimension.alias) for dimension in dimensions
        ]

        items = [
            item if reference is None else ReferenceItem(item, reference)
            for item in self.items
            for reference in [None] + references
        ]

        if isinstance(result_df.index, pd.MultiIndex):
            result_df = result_df.reorder_levels(dimension_aliases)

        result_df = result_df[[alias_selector(item.alias) for item in items]]

        hide_dimensions = set(self.hide) | {
            dimension for dimension in dimensions if dimension.fetch_only
        }
        self.hide_data_frame_indexes(result_df, hide_dimensions)

        hide_aliases = {
            dimension.alias for dimension in hide_dimensions
        }

        if dimensions:
            result_df.index.names = [
                dimension.label or dimension.alias
                for dimension in dimensions
                if dimension.alias not in hide_aliases
            ]

        result_df.columns = pd.Index([item.label for item in items], name="Metrics")

        pivot_dimensions = [
            dimension.label or dimension.alias
            for dimension in self.pivot
            if dimension.alias not in hide_aliases
        ]
        result_df, _, _ = self.pivot_data_frame(result_df, pivot_dimensions, self.transpose)
        return self.add_formatting(dimensions, items, result_df, use_raw_values).fillna(
            value=formats.BLANK_VALUE
        )

    @staticmethod
    def _should_data_frame_be_transformed(data_frame, pivot_dimensions, transpose):
        if not pivot_dimensions and not transpose:
            return False

        if transpose and len(pivot_dimensions) == len(data_frame.index.names):
            # Pivot and transpose cancel each other out
            return False

        return True

    def pivot_data_frame(self, data_frame, pivot_dimensions, transpose):
        """
        Pivot and transpose the data frame. Dimensions including in the `pivot` arg will be unshifted to columns. If
        `transpose` is True the data frame will be transposed. If there is only index level in the data frame (ie. one
        dimension), and that dimension is pivoted, then the data frame will just be transposed. If there is a single
        metric in the data frame and at least one dimension pivoted, the metrics column level will be dropped for
        simplicity.

        :param data_frame:
            The result set data frame
        :param pivot_dimensions:
            A list of index aliases for `data_frame` of levels to shift
        :param transpose:
            A boolean true or false whether to transpose the data frame.
        :return:
            Tuple(The shifted/transposed data frame, is_pivoted, is_transposed)
        """
        is_pivoted = False
        is_transposed = False

        if not self._should_data_frame_be_transformed(data_frame, pivot_dimensions, transpose):
            return self.sort_data_frame(data_frame), is_pivoted, is_transposed

        # NOTE: Don't pivot a single dimension data frame. This turns the data frame into a series and pivots the
        # metrics anyway. Instead, transpose the data frame.
        should_transpose_instead_of_pivot = len(pivot_dimensions) == len(data_frame.index.names)

        if pivot_dimensions and not should_transpose_instead_of_pivot:
            data_frame = data_frame.unstack(level=pivot_dimensions)
            is_pivoted = True

        if transpose or should_transpose_instead_of_pivot:
            data_frame = data_frame.transpose()
            is_transposed = True

        # If there are more than one column levels and the last level is a single metric, drop the level
        if isinstance(data_frame.columns, pd.MultiIndex) and 1 == len(
            data_frame.columns.levels[0]
        ):
            data_frame.name = data_frame.columns.levels[0][
                0
            ]  # capture the name of the metrics column
            data_frame.columns = data_frame.columns.droplevel(
                0
            )  # drop the metrics level

        return self.sort_data_frame(data_frame), is_pivoted, is_transposed

    def sort_data_frame(self, data_frame):
        if not self.sort or len(data_frame) == 1:
            # If there are no sort arguments or the data frame is a single row, then no need to sort
            return data_frame

        # reset the index so all columns can be sorted together
        index_names = data_frame.index.names
        unsorted = data_frame.reset_index()
        column_names = list(unsorted.columns)

        ascending = self.ascending if self.ascending is not None else True

        sort = wrap_list(self.sort)
        sort_columns = [
            column_names[column] for column in sort if column < len(column_names)
        ]

        if not sort_columns:
            return data_frame

        # ignore additional values for ascending if they do not align lengthwise with sort_columns
        # Default to the first value or None
        if isinstance(ascending, list) and len(ascending) != len(sort_columns):
            ascending = ascending[0] if len(ascending) > 0 else None

        sorted = unsorted.sort_values(sort_columns, ascending=ascending).set_index(
            index_names
        )

        # Maintain the single metric name
        if hasattr(data_frame, "name"):
            sorted.name = data_frame.name

        return sorted

    def add_formatting(self, dimensions, items, pivot_df, use_raw_values):
        format_df = pivot_df.copy()

        def _get_f_display(item):
            return partial(
                formats.display_value,
                field=item,
                nan_value="",
                null_value="",
                use_raw_value=use_raw_values,
            )

        if (
            self.transpose
            or not self.transpose
            and len(dimensions) == len(self.pivot) > 0
        ):
            for item in items:
                f_display = _get_f_display(item)
                format_df.loc[items[0].label] = format_df.loc[items[0].label].apply(
                    f_display
                )

            return format_df

        if self.pivot and len(items) == 1:
            f_display = _get_f_display(items[0])
            format_df = format_df.applymap(f_display)
            return format_df

        for item in items:
            key = item.label
            f_display = _get_f_display(item)
            format_df[key] = (
                format_df[key].apply(f_display)
                if isinstance(format_df[key], pd.Series)
                else format_df[key].applymap(f_display)
            )

        return format_df
