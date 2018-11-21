import time
from functools import (
    partial,
    reduce,
    wraps,
)
from multiprocessing.pool import ThreadPool

import pandas as pd
from typing import (
    Iterable,
    Sized,
    Union,
)

from fireant.database import Database
from fireant.formats import (
    NULL_VALUE,
    TOTALS_VALUE,
)
from fireant.utils import (
    chunks,
    format_dimension_key,
)
from .logger import (
    query_logger,
    slow_query_logger,
)
from ..dimensions import (
    ContinuousDimension,
    Dimension,
)


def fetch_data(database: Database, queries: Union[Sized, Iterable], dimensions: Iterable[Dimension],
               reference_groups=()):
    iterable = [(str(query.limit(int(database.max_result_set_size))), database, dimensions)
                for query in queries]

    with ThreadPool(processes=database.max_processes) as pool:
        results = pool.map(_exec, iterable)
        pool.close()

    return _reduce_result_set(results, reference_groups)


def _exec(args):
    return _do_fetch_data(*args)


def db_cache(func):
    @wraps(func)
    def wrapper(query, database, *args):
        if database.cache_middleware is not None:
            return database.cache_middleware(func)(query, database, *args)
        return func(query, database, *args)

    return wrapper


def log(func):
    @wraps(func)
    def wrapper(query, database, *args):
        start_time = time.time()
        query_logger.debug(query)

        result = func(query, database, *args)

        duration = round(time.time() - start_time, 4)
        query_log_msg = '[{duration} seconds]: {query}'.format(duration=duration,
                                                               query=query)
        query_logger.info(query_log_msg)

        if database.slow_query_log_min_seconds is not None and duration >= database.slow_query_log_min_seconds:
            slow_query_logger.warning(query_log_msg)

        return result

    return wrapper


@db_cache
@log
def _do_fetch_data(query: str, database: Database, dimensions: Iterable[Dimension]):
    """
    Executes a query to fetch data from database middleware and builds/cleans the data as a data frame. The query
    execution is logged with its duration.

    :param database:
        instance of `fireant.Database`, database middleware
    :param query: Query string
    :param dimensions: A list of dimensions, used for setting the index on the result data frame.

    :return: `pd.DataFrame` constructed from the result of the query
    """
    with database.connect() as connection:
        data_frame = pd.read_sql(query, connection, coerce_float=True, parse_dates=True)
        return _clean_and_apply_index(data_frame, dimensions)


def _reduce_result_set(results: Iterable[pd.DataFrame], reference_groups=()):
    """
    Reduces the result sets from individual queries into a single data frame. This effectively joins sets of references
    and concats the sets of totals.

    :param results: A list of data frame
    :param reference_groups: A list of groups of references (grouped by interval such as WoW, etc)
    :return:
    """
    result_groups = chunks(results, 1 + len(reference_groups))

    groups = []
    for result_group in result_groups:
        base_df = result_group[0]
        reference_dfs = [_make_reference_data_frame(base_df, result, reference)
                         for result, reference_group in zip(result_group[1:], reference_groups)
                         for reference in reference_group]

        reduced = reduce(lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True),
                         [base_df] + reference_dfs)
        groups.append(reduced)

    return pd.concat(groups, sort=False)


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
    mertric_column_indices = [i
                              for i, column in enumerate(ref_df.columns)
                              if column not in base_df.columns]
    ref_columns = [ref_df.columns[i] for i in mertric_column_indices]

    if not (reference.delta or reference.delta_percent):
        return ref_df[ref_columns]

    base_columns = [base_df.columns[i] for i in mertric_column_indices]

    # Select just the metric columns from the DF and rename them with the reference key as a suffix
    base_df, ref_df = base_df[base_columns].copy(), ref_df[ref_columns].copy()
    # Both data frame columns are renamed in order to perform the calculation below.
    base_df.columns = ref_df.columns = [column.replace(reference.reference_type.key, reference.key)
                                        for column in ref_columns]

    ref_delta_df = base_df - ref_df

    if reference.delta_percent:
        return 100. * ref_delta_df / ref_df
    return ref_delta_df


def _clean_and_apply_index(data_frame: pd.DataFrame, dimensions: Iterable[Dimension]):
    """
    Sets the index on a data frame. This will also replace any nulls from the database with an empty string for
    non-continuous dimensions. Totals will be indexed with Nones.

    :param data_frame:
    :param dimensions:
    :return:
    """
    if not dimensions:
        return data_frame

    dimension_keys = [format_dimension_key(d.key)
                      for d in dimensions]

    for i, dimension in enumerate(dimensions):
        if isinstance(dimension, ContinuousDimension):
            # Continuous dimensions are can never contain null values since they are selected as windows of values
            # With that in mind, we leave the NaNs in them to represent Totals.
            continue

        level = format_dimension_key(dimension.key)
        data_frame[level] = _fill_nans_in_level(data_frame, dimensions[:i + 1]) \
            .apply(
              # Handles an annoying case of pandas in which the ENTIRE data frame gets converted from int to float if
              # the are NaNs, even if there are no NaNs in the column :/
              lambda x: int(x) if isinstance(x, float) and float.is_integer(x) else x) \
            .apply(lambda x: str(x) if not pd.isnull(x) else None)

    # Set index on dimension columns
    return data_frame.set_index(dimension_keys)


def _fill_nans_in_level(data_frame, dimensions):
    """
    In case there are NaN values representing both totals (from ROLLUP) and database nulls, we need to replace the real
    nulls with an empty string in order to distinguish between them.  We choose to replace the actual database nulls
    with an empty string in order to let NaNs represent Totals because mixing string values in the pandas index types
    used by continuous dimensions does work.

    :param data_frame:
        The data_frame we are replacing values in.
    :param dimensions:
        A list of dimensions with the last item in the list being the dimension to fill nans for. This function requires
        the dimension being processed as well as the preceding dimensions since a roll up in a higher level dimension
        results in nulls for lower level dimension.
    :return:
        The level in the data_frame with the nulls replaced with empty string
    """
    level = format_dimension_key(dimensions[-1].key)

    number_rollup_dimensions = sum(dimension.is_rollup for dimension in dimensions)
    if 0 < number_rollup_dimensions:
        fill_nan_for_nulls = partial(_fill_nan_for_nulls, n_rolled_up_dimensions=number_rollup_dimensions)

        if 1 < len(dimensions):
            preceding_dimension_keys = [format_dimension_key(d.key)
                                        for d in dimensions[:-1]]

            return (data_frame
                    .groupby(preceding_dimension_keys)[level]
                    .apply(fill_nan_for_nulls))

        return fill_nan_for_nulls(data_frame[level])

    return data_frame[level].fillna(NULL_VALUE)


def _fill_nan_for_nulls(df, n_rolled_up_dimensions=1):
    """
    Fills the first NaN with a literal string "null" if there are two NaN values, otherwise nothing is filled.

    :param df:
    :param n_rolled_up_dimensions:
        The number of rolled up dimensions preceding and including the dimension
    :return:
    """

    # If there are rolled up dimensions, then fill only the first instance of NULL with a literal string "null" and
    # the rest of the nulls are totals. This check compares the number of nulls to the number of rolled up dimensions,
    # or expected nulls which are totals rows. If there are more nulls, there should be exactly
    # `n_rolled_up_dimensions+1` nulls which means one is a true `null` value.
    number_of_nulls_for_dimension = pd.isnull(df).sum()
    if n_rolled_up_dimensions < number_of_nulls_for_dimension:
        assert n_rolled_up_dimensions + 1 == number_of_nulls_for_dimension
        return df.fillna(NULL_VALUE, limit=1).fillna(TOTALS_VALUE)

    return df.fillna(TOTALS_VALUE)
