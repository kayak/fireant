import pandas as pd

from collections import OrderedDict
from functools import partial
from typing import Iterable, List, Optional, Tuple, Union

from fireant import formats
from fireant.dataset.fields import DataType, Field
from fireant.dataset.references import Reference
from fireant.utils import alias_selector, wrap_list
from .base import ReferenceItem, TransformableWidget, HideField
from fireant.dataset.totals import DATE_TOTALS, NUMBER_TOTALS, TEXT_TOTALS
from fireant.formats import TOTALS_LABEL, TOTALS_VALUE
from fireant.reference_helpers import reference_alias

HARD_MAX_COLUMNS = 24

METRICS_DIMENSION_ALIAS = "metrics"
F_METRICS_DIMENSION_ALIAS = alias_selector(METRICS_DIMENSION_ALIAS)


class TotalsItem:
    alias = TOTALS_VALUE
    label = TOTALS_LABEL
    prefix = suffix = precision = None


class Pandas(TransformableWidget):
    def __init__(
        self,
        metric: Field,
        *metrics: Iterable[Field],
        pivot: Optional[List[Field]] = (),
        hide: Optional[List[HideField]] = None,
        transpose: bool = False,
        sort=None,
        ascending: Optional[bool] = None,
        max_columns: Optional[int] = None,
    ):
        super(Pandas, self).__init__(metric, *metrics, hide=hide)
        self.pivot = pivot
        self.transpose = transpose
        self.sort = sort
        self.ascending = ascending
        self.max_columns = min(max_columns, HARD_MAX_COLUMNS) if max_columns is not None else HARD_MAX_COLUMNS

    def transform(
        self,
        data_frame: pd.DataFrame,
        dimensions: List[Field],
        references: List[Reference],
        annotation_frame: Optional[pd.DataFrame] = None,
        use_raw_values: bool = False,
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

        dimension_map = {alias_selector(dimension.alias): dimension for dimension in dimensions}
        dimension_aliases = dimension_map.keys()
        metric_map = OrderedDict(
            [
                (
                    alias_selector(reference_alias(item, ref)),
                    ReferenceItem(item, ref) if ref is not None else item,
                )
                for item in self.items
                for ref in [None] + references
            ]
        )
        metric_aliases = metric_map.keys()

        field_map = {
            **metric_map,
            **dimension_map,
            # Add an extra item to map the totals markers to it's label
            NUMBER_TOTALS: TotalsItem,
            TEXT_TOTALS: TotalsItem,
            DATE_TOTALS: TotalsItem,
            TOTALS_LABEL: TotalsItem,
            alias_selector(METRICS_DIMENSION_ALIAS): Field(
                METRICS_DIMENSION_ALIAS, None, data_type=DataType.text, label=""
            ),
        }

        if isinstance(result_df.index, pd.MultiIndex):
            result_df = result_df.reorder_levels(dimension_aliases)

        result_df = result_df[metric_aliases]

        hide_aliases = self.hide_aliases(dimensions)

        if dimensions:
            result_df.index.names = [
                alias_selector(dimension.alias) for dimension in dimensions if dimension.alias not in hide_aliases
            ]

        self.hide_data_frame_indexes(result_df, hide_aliases)

        result_df.columns.name = 'Metrics'

        pivot_dimensions = [
            alias_selector(dimension.alias) for dimension in self.pivot if dimension.alias not in hide_aliases
        ]
        result_df, _, _ = self.pivot_data_frame(result_df, pivot_dimensions, self.transpose)

        metrics = [metric for metric_alias, metric in metric_map.items() if metric_alias not in hide_aliases]
        result_df = self.add_formatting(dimensions, metrics, result_df, use_raw_values).fillna(
            value=formats.BLANK_VALUE
        )
        return self.transform_df_schema(result_df, field_map)

    def transform_df_schema(self, data_frame: pd.DataFrame, field_map: dict) -> pd.DataFrame:
        data_frame.index.names = self._transform_index_values(data_frame.index.names, field_map)
        data_frame.columns.names = self._transform_index_values(data_frame.columns.names, field_map)
        data_frame.index = self._build_index(data_frame.index, field_map)
        data_frame.columns = self._build_index(data_frame.columns, field_map)
        return data_frame

    @staticmethod
    def _build_index(idx: Union[pd.Index, pd.MultiIndex], field_map: dict) -> Union[pd.Index, pd.MultiIndex]:
        if isinstance(idx, pd.MultiIndex):
            return pd.MultiIndex.from_tuples(
                [Pandas._transform_index_values(level, field_map) for level in idx.tolist()], names=idx.names
            )

        new_idx = Pandas._transform_index_values(idx.tolist(), field_map)
        return pd.Index(new_idx, name=idx.name)

    @staticmethod
    def _transform_index_values(idx: List[str], field_map: dict) -> List[str]:
        return [field_map[item].label if item in field_map else item for item in idx]

    @staticmethod
    def _should_data_frame_be_transformed(
        data_frame: pd.DataFrame, pivot_dimensions: List[str], transpose: bool
    ) -> bool:
        if not pivot_dimensions and not transpose:
            return False

        if transpose and len(pivot_dimensions) == len(data_frame.index.names):
            # Pivot and transpose cancel each other out
            return False

        return True

    def pivot_data_frame(
        self, data_frame: pd.DataFrame, pivot_dimensions: List[str], transpose: bool
    ) -> Tuple[pd.DataFrame, bool, bool]:
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
        if isinstance(data_frame.columns, pd.MultiIndex) and 1 == len(data_frame.columns.levels[0]):
            data_frame.name = data_frame.columns.levels[0][0]  # capture the name of the metrics column
            data_frame.columns = data_frame.columns.droplevel(0)  # drop the metrics level

        return self.sort_data_frame(data_frame), is_pivoted, is_transposed

    def sort_data_frame(self, data_frame: pd.DataFrame):
        if not self.sort or len(data_frame) == 1:
            # If there are no sort arguments or the data frame is a single row, then no need to sort
            return data_frame

        # reset the index so all columns can be sorted together
        index_names = data_frame.index.names
        unsorted = data_frame.reset_index()
        column_names = list(unsorted.columns)

        ascending = self.ascending if self.ascending is not None else True

        sort = wrap_list(self.sort)
        sort_columns = [column_names[column] for column in sort if column < len(column_names)]

        if not sort_columns:
            return data_frame

        # ignore additional values for ascending if they do not align lengthwise with sort_columns
        # Default to the first value or None
        if isinstance(ascending, list) and len(ascending) != len(sort_columns):
            ascending = ascending[0] if len(ascending) > 0 else None

        sorted = unsorted.sort_values(sort_columns, ascending=ascending).set_index(index_names)

        # Maintain the single metric name
        if hasattr(data_frame, "name"):
            sorted.name = data_frame.name

        return sorted

    def add_formatting(
        self, dimensions: List[Field], items: List[Field], pivot_df: pd.DataFrame, use_raw_values: bool
    ) -> pd.DataFrame:
        format_df = pivot_df.copy()

        def _get_field_display(item):
            return partial(
                formats.display_value,
                field=item,
                nan_value="",
                null_value="",
                use_raw_value=use_raw_values,
            )

        if self.transpose or not self.transpose and len(dimensions) == len(self.pivot) > 0:
            for item in items:
                field_display = _get_field_display(item)
                alias = alias_selector(items[0].alias)
                format_df.loc[alias] = format_df.loc[alias].apply(field_display)

            return format_df

        if self.pivot and len(items) == 1:
            field_display = _get_field_display(items[0])
            format_df = format_df.applymap(field_display)
            return format_df

        for item in items:
            key = alias_selector(item.alias)
            field_display = _get_field_display(item)
            format_df[key] = (
                format_df[key].apply(field_display)
                if isinstance(format_df[key], pd.Series)
                else format_df[key].applymap(field_display)
            )

        return format_df
