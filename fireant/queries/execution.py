from functools import reduce
from typing import (
    Iterable,
    Sized,
    Union,
)

import pandas as pd

from fireant.database import Database
from fireant.dataset.fields import Field
from fireant.dataset.references import calculate_delta_percent
from fireant.dataset.totals import get_totals_marker_for_dtype

from fireant.utils import (
    alias_selector,
    chunks,
)
from .finders import find_totals_dimensions
from .pandas_workaround import df_subtract


def fetch_data(
    database: Database,
    queries: Union[Sized, Iterable],
    dimensions: Iterable[Field],
    share_dimensions: Iterable[Field] = (),
    reference_groups=(),
):
    queries = [
        str(
            query.limit(min(query._limit or float("inf"), database.max_result_set_size))
        )
        for query in queries
    ]
    results = database.fetch_dataframes(*queries)
    return reduce_result_set(results, reference_groups, dimensions, share_dimensions)


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

        reduced = reduce(
            lambda left, right: pd.merge(
                left, right, how="outer", left_index=True, right_index=True
            ),
            [base_df] + reference_dfs,
        )

        # If there are rolled up dimensions in this result set then replace the NaNs for that dimension value with a
        # marker to indicate totals.
        # The data frames will be ordered so that the first group will contain the data without any rolled up
        # dimensions, then followed by the groups with them, ordered by the last rollup dimension first.
        if totals_dimension_keys[:i]:
            reduced = _replace_nans_for_totals_values(reduced, dimension_dtypes)

        group_data_frames.append(reduced)

    return pd.concat(group_data_frames, sort=False).sort_index(na_position="first")


def _replace_nans_for_totals_values(data_frame, dtypes):
    # some things are just easier to do without an index. Reset it temporarily to replace NaN values with the rollup
    # marker values
    index_names = data_frame.index.names
    data_frame.reset_index(inplace=True)

    for dimension_key, dtype in dtypes.items():
        data_frame[dimension_key] = data_frame[dimension_key].fillna(
            get_totals_marker_for_dtype(dtype)
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
    mertric_column_indices = [
        i for i, column in enumerate(ref_df.columns) if column not in base_df.columns
    ]
    ref_columns = [ref_df.columns[i] for i in mertric_column_indices]

    if not (reference.delta or reference.delta_percent):
        return ref_df[ref_columns]

    base_columns = [base_df.columns[i] for i in mertric_column_indices]

    # Select just the metric columns from the DF and rename them with the reference key as a suffix
    base_df, ref_df = base_df[base_columns].copy(), ref_df[ref_columns].copy()
    # Both data frame columns are renamed in order to perform the calculation below.
    base_df.columns = ref_df.columns = [
        column.replace(reference.reference_type.alias, reference.alias)
        for column in ref_columns
    ]

    ref_delta_df = df_subtract(base_df, ref_df, fill_value=0)

    if reference.delta_percent:
        return calculate_delta_percent(ref_df, ref_delta_df)
    return ref_delta_df
