from functools import (
    reduce,
    wraps,
)
from multiprocessing.pool import ThreadPool
from typing import (
    Iterable,
    Sized,
    Union,
)

import pandas as pd
import time

from fireant.database import Database
from fireant.dataset.fields import Field
from fireant.dataset.totals import get_totals_marker_for_dtype
from fireant.utils import (
    alias_selector,
    chunks,
)
from .finders import find_totals_dimensions
from .slow_query_logger import (
    query_logger,
    slow_query_logger,
)


def fetch_data(database: Database,
               queries: Union[Sized, Iterable],
               dimensions: Iterable[Field],
               share_dimensions: Iterable[Field] = (),
               reference_groups=()):
    iterable = [(str(query.limit(int(database.max_result_set_size))), database)
                for query in queries]

    with ThreadPool(processes=database.max_processes) as pool:
        results = pool.map(_exec, iterable)
        pool.close()

    return reduce_result_set(results, reference_groups, dimensions, share_dimensions)


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
def _do_fetch_data(query: str, database: Database):
    """
    Executes a query to fetch data from database middleware and builds/cleans the data as a data frame. The query
    execution is logged with its duration.

    :param database:
        instance of `fireant.Database`, database middleware
    :param query: Query string

    :return: `pd.DataFrame` constructed from the result of the query
    """
    with database.connect() as connection:
        return pd.read_sql(query, connection, coerce_float=True, parse_dates=True)


def reduce_result_set(results: Iterable[pd.DataFrame],
                      reference_groups,
                      dimensions: Iterable[Field],
                      share_dimensions: Iterable[Field]):
    """
    Reduces the result sets from individual queries into a single data frame. This effectively joins sets of references
    and concats the sets of totals.

    :param results: A list of data frame
    :param reference_groups: A list of groups of references (grouped by interval such as WoW, etc)
    :param dimensions: A list of dimensions, used for setting the index on the result data frame.
    :param share_dimensions: A list of dimensions from which the totals are used for calculating share operations.
    :return:
    """

    # One result group for each rolled up dimension. Groups contain one member plus one for each reference type used.
    result_groups = chunks(results, 1 + len(reference_groups))

    dimension_keys = [alias_selector(d.alias)
                      for d in dimensions]
    totals_dimension_keys = [alias_selector(d.alias)
                             for d in find_totals_dimensions(dimensions, share_dimensions)]
    dimension_dtypes = result_groups[0][0][dimension_keys].dtypes

    # Reduce each group to one data frame per rolled up dimension
    group_data_frames = []
    for i, result_group in enumerate(result_groups):
        if dimension_keys:
            result_group = [result.set_index(dimension_keys)
                            for result in result_group]

        base_df = result_group[0]
        reference_dfs = [_make_reference_data_frame(base_df, result, reference)
                         for result, reference_group in zip(result_group[1:], reference_groups)
                         for reference in reference_group]

        reduced = reduce(lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True),
                         [base_df] + reference_dfs)

        # If there are rolled up dimensions in this result set then replace the NaNs for that dimension value with a
        # marker to indicate totals.
        # The data frames will be ordered so that the first group will contain the data without any rolled up
        # dimensions, then followed by the groups with them, ordered by the last rollup dimension first.
        if totals_dimension_keys[:i]:
            reduced = _replace_nans_for_totals_values(reduced, dimension_dtypes)

        group_data_frames.append(reduced)

    return pd.concat(group_data_frames, sort=False) \
        .sort_index(na_position='first')


def _replace_nans_for_totals_values(data_frame, dtypes):
    # some things are just easier to do without an index. Reset it temporarily to replaxe NaN values with the rollup
    # marker values
    index_names = data_frame.index.names
    data_frame.reset_index(inplace=True)

    for dimension_key, dtype in dtypes.items():
        data_frame[dimension_key] = data_frame[dimension_key].fillna(get_totals_marker_for_dtype(dtype))

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
    base_df.columns = ref_df.columns = [column.replace(reference.reference_type.alias, reference.alias)
                                        for column in ref_columns]

    ref_delta_df = base_df - ref_df

    if reference.delta_percent:
        return 100. * ref_delta_df / ref_df
    return ref_delta_df
