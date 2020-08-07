from typing import Iterable, List, Tuple, Type

import pandas as pd
from pypika.queries import QueryBuilder

from fireant.database import Database
from fireant.dataset.fields import DataType, Field
from fireant.dataset.references import calculate_delta_percent
from fireant.dataset.totals import get_totals_marker_for_dtype
from fireant.utils import alias_selector, chunks
from .finders import find_field_in_modified_field, find_totals_dimensions
from .pandas_workaround import df_subtract
from ..dataset.modifiers import RollupValue

# Passing in an empty dictionary as format option to pandas' parse_dates causes errors to be ignored instead of
# being coerced into NaT.
PANDAS_TO_DATETIME_FORMAT = {}


def fetch_data(
    database: Database,
    queries: List[Type[QueryBuilder]],
    dimensions: Iterable[Field],
    share_dimensions: Iterable[Field] = (),
    reference_groups=(),
) -> Tuple[int, pd.DataFrame]:
    queries = [str(query) for query in queries]

    # Indicate which dimensions need to be parsed as date types
    # For this we create a dictionary with the dimension alias as key and PANDAS_TO_DATETIME_FORMAT as value
    pandas_parse_dates = {}
    for dimension in dimensions:
        unmodified_dimension = find_field_in_modified_field(dimension)
        if unmodified_dimension.data_type == DataType.date:
            pandas_parse_dates[alias_selector(unmodified_dimension.alias)] = PANDAS_TO_DATETIME_FORMAT

    results = database.fetch_dataframes(*queries, parse_dates=pandas_parse_dates)
    max_rows_returned = max([len(x) for x in results], default=0)

    return max_rows_returned, reduce_result_set(results, reference_groups, dimensions, share_dimensions)


def reduce_result_set(
    results: Iterable[pd.DataFrame],
    reference_groups,
    dimensions: Iterable[Field],
    share_dimensions: Iterable[Field],
):
    """
    Reduces the result sets from individual queries into a single data frame. This effectively joins sets of references
    and concatenates the sets of totals.

    :param results: A list of data frame
    :param reference_groups: A list of groups of references (grouped by interval such as WoW, etc)
    :param dimensions: A list of dimensions, used for setting the index on the result data frame.
    :param share_dimensions: A list of dimensions from which the totals are used for calculating share operations.
    :return:
    """
    # One result group for each rolled up dimension. Groups contain one member plus one for each reference type used.
    result_groups = chunks(results, 1 + len(reference_groups))

    dimension_keys = [alias_selector(d.alias) for d in dimensions]
    totals_dimension_keys = [
        alias_selector(d.alias)
        for d in find_totals_dimensions(dimensions, share_dimensions)
    ]
    dimension_dtypes = result_groups[0][0][dimension_keys].dtypes

    # Reduce each group to one data frame per rolled up dimension
    group_data_frames = []
    for i, result_group in enumerate(result_groups):
        if dimension_keys:
            result_group = [result.set_index(dimension_keys) for result in result_group]

        base_df = result_group[0]
        reference_dfs = [
            _make_reference_data_frame(base_df, result, reference)
            for result, reference_group in zip(result_group[1:], reference_groups)
            for reference in reference_group
        ]

        # Merge the different reference dataframes into 1
        merged_df = base_df
        for reference_df in reference_dfs:
            merged_df = pd.merge(
                merged_df, reference_df, how="outer", left_index=True, right_index=True, suffixes=('', '_delete')
            )
            # Drop duplicate columns from the merge (indicate by suffix _delete)
            merged_df.drop(merged_df.filter(regex='_delete$').columns.tolist(), axis=1, inplace=True)

        # If there are rolled up dimensions in this result set then replace the NaNs for that dimension value with a
        # marker to indicate totals.
        # The data frames will be ordered so that the first group will contain the data without any rolled up
        # dimensions, then followed by the groups with them, ordered by the last rollup dimension first.
        if totals_dimension_keys[:i]:
            merged_df = _replace_rollup_constants_for_totals_markers(merged_df, dimension_dtypes)

        group_data_frames.append(merged_df)

    return pd.concat(group_data_frames, sort=False).sort_index(na_position="first")


def _replace_rollup_constants_for_totals_markers(data_frame, dtypes):
    # some things are just easier to do without an index. Reset it temporarily to replace Rollup constants with the
    # rollup marker values
    index_names = data_frame.index.names
    data_frame.reset_index(inplace=True)

    for dimension_key, dtype in dtypes.items():
        data_frame[dimension_key] = data_frame[dimension_key].replace(
            RollupValue.CONSTANT, get_totals_marker_for_dtype(dtype)
        )

    return data_frame.set_index(index_names)


def _make_reference_data_frame(base_df, ref_df, reference):
    """
    This applies the reference metrics to the data frame given the base data frame and the reference data frame.

    When a reference is selected as a delta or a delta percentage, the calculation is performed here. Otherwise, the
    reference data frame is returned.

    :param base_df:
    :param ref_df:
    :param reference:
    :return:
    """
    metric_column_indices = [
        i for i, column in enumerate(ref_df.columns) if column not in base_df.columns
    ]
    ref_columns = [ref_df.columns[i] for i in metric_column_indices]

    if not (reference.delta or reference.delta_percent):
        return ref_df[ref_columns]

    original_ref_df = ref_df

    base_columns = [base_df.columns[i] for i in metric_column_indices]

    # Select just the metric columns from the DF and rename them with the reference key as a suffix
    base_df, ref_df = base_df[base_columns].copy(), ref_df[ref_columns].copy()
    # Both data frame columns are renamed in order to perform the calculation below.
    base_df.columns = ref_df.columns = [
        column.replace(reference.reference_type.alias, reference.alias)
        for column in ref_columns
    ]

    ref_delta_df = df_subtract(base_df, ref_df, fill_value=0)

    if reference.delta_percent:
        ref_delta_df = calculate_delta_percent(ref_df, ref_delta_df)

    # Add original reference values back to df
    ref_delta_df[ref_columns] = original_ref_df[ref_columns]
    # Alternative for the above line but with copying:
    # ref_delta_df = ref_delta_df.join(original_ref_df[ref_columns])

    return ref_delta_df
