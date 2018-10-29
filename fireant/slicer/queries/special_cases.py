import functools

import pandas as pd
from dateutil.relativedelta import relativedelta

from fireant.slicer.dimensions import DatetimeDimension
from fireant.slicer.filters import RangeFilter
from fireant.slicer.operations import RollingOperation


def adjust_daterange_filter_for_rolling_window(dimensions, operations, filters):
    """
    This function adjusts date filters for a rolling operation in order to select enough date to compute the values for
    within the original range.

    It only applies when using a date dimension in the first position and a RangeFilter is used on that dimension. It
    is meant to be applied to a slicer query.

    :param dimensions:
        The dimensions applied to a slicer query
    :param operations:
        The dimensions used in widgets in a slicer query
    :param filters:
        The filters applied to a slicer query
    :return:
    """
    has_datetime_dimension_in_first_dimension_pos = not len(dimensions) \
                                                    or not isinstance(dimensions[0], DatetimeDimension)
    if has_datetime_dimension_in_first_dimension_pos:
        return filters

    has_rolling = any([isinstance(operation, RollingOperation)
                       for operation in operations])
    if not has_rolling:
        return filters

    dim0 = dimensions[0]
    filters_on_dim0 = [filter_
                       for filter_ in filters
                       if isinstance(filter_, RangeFilter)
                       and str(filter_.definition.term) == str(dim0.definition)]
    if not 0 < len(filters_on_dim0):
        return filters

    max_rolling_period = max(operation.window
                             for operation in operations
                             if isinstance(operation, RollingOperation))

    for filter_ in filters_on_dim0:
        # Monkey patch the update start date on the date filter
        args = {dim0.interval + 's': max_rolling_period} \
            if 'quarter' != dim0.interval \
            else {'months': max_rolling_period * 3}
        filter_.definition.start.value -= relativedelta(**args)

    return filters


def adjust_dataframe_for_rolling_window(operations, data_frame):
    """
    This function adjusts the resulting data frame after executing a slicer query with a rolling operation. If there is
    a date dimension in the first level of the data frame's index and a rolling operation is applied, it will slice the
    dates following the max window to remove it. This way, the adjustment of date filters applied in
    #adjust_daterange_filter_for_rolling_window are removed from the data frame but also in case there are no filters,
    the first few date data points will be removed where the rolling window cannot be calculated.

    :param operations:
    :param data_frame:
    :return:
    """
    has_rolling = any([isinstance(operation, RollingOperation)
                       for operation in operations])
    if not has_rolling:
        return data_frame

    max_rolling_period = max(operation.window
                             for operation in operations
                             if isinstance(operation, RollingOperation))

    if isinstance(data_frame.index, pd.DatetimeIndex):
        return data_frame.iloc[max_rolling_period - 1:]

    if isinstance(data_frame.index, pd.MultiIndex) \
          and isinstance(data_frame.index.levels[0], pd.DatetimeIndex):
        num_levels = len(data_frame.index.levels)

        return data_frame.groupby(level=list(range(1, num_levels))) \
            .apply(lambda df: df.iloc[max_rolling_period - 1:]) \
            .reset_index(level=list(range(num_levels - 1)), drop=True)

    return data_frame


def apply_to_query_args(database, table, joins, dimensions, metrics, operations, filters, references, orders):
    filters = adjust_daterange_filter_for_rolling_window(dimensions, operations, filters)
    return (database, table, joins, dimensions, metrics, operations, filters, references, orders)


def apply_special_cases(f):
    @functools.wraps(f)
    def wrapper(*args):
        return f(*apply_to_query_args(*args))

    return wrapper


def apply_operations_to_data_frame(operations, data_frame):
    data_frame = adjust_dataframe_for_rolling_window(operations, data_frame)

    return data_frame
