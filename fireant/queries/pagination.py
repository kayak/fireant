from typing import Tuple

import pandas as pd
from pandas.core.dtypes.common import is_datetime64_ns_dtype
from pypika import Order

from fireant.utils import alias_selector


def _get_window(limit, offset):
    start = offset
    end = offset + limit if None not in (offset, limit) else limit
    return start, end


def _get_sorting_schema(orders) -> Tuple[list, bool]:
    sort_values, ascending = zip(
        *[
            (alias_selector(field.alias), orientation is Order.asc)
            for field, orientation in orders
        ]
    )
    return list(sort_values), ascending


def paginate(data_frame, widgets, orders=(), limit=None, offset=None):
    """
    :param data_frame:
        The result set to sort.
    :param widgets:
        An iterable of widgets that the sort is being applied for.
    :param orders:
        An iterable of (<Dimension/Metric>, pypika.Order)
     :param limit:
        A limit of the number of data points/series
    :param offset:
        A offset of the number of data points/series
    :return:
        A paginated data frame. If the widget required grouped pagination, then there should be an upperbound
        `limit*(n_index_level_0)`. Otherwise the data frame should have the same length as the limit.
    :return:
        A sorted data frame.
    """
    if len(data_frame) == 0:
        return data_frame

    start, end = _get_window(limit, offset)

    needs_group_pagination = isinstance(data_frame.index, pd.MultiIndex) and any(
        [getattr(widget, 'group_pagination', False) for widget in widgets]
    )

    if needs_group_pagination:
        return _group_paginate(data_frame, start, end, orders)
    return _simple_paginate(data_frame, start, end, orders)


def _simple_paginate(data_frame, start=None, end=None, orders=()):
    """
    Applies pagination which limits the number of rows in the dataframe.

    :param data_frame:
        A data frame to paginate
    :param start:
        The index starting point to slice the data frame at
    :param end:
        The index ending point to slice the data frame at
    :param orders:
        A list of tuples that contain a slicer field definition (with an alias matching the columns of the data frame)
        and a pypika.Order.
    """
    if orders:
        sort, ascending = _get_sorting_schema(orders)
        data_frame = data_frame.sort_values(by=sort, ascending=ascending)

    return data_frame[start:end]


def _index_isnull(data_frame):
    if isinstance(data_frame.index, pd.MultiIndex):
        return [
            any(pd.isnull(value) for value in level) for level in list(data_frame.index)
        ]

    return pd.isnull(data_frame.index)


def _aggregate_dimension_groups(group):
    # FIXME this should aggregate according to field definition, instead of sum/max
    # Need a way to interpret definitions in python code in order to do that
    if is_datetime64_ns_dtype(group):
        # sum aggregation doesn't work on the datetime type so use max instead
        return group.max()
    return group.sum()


def _group_paginate(data_frame, start=None, end=None, orders=()):
    """
    Applies pagination which limits the number of rows in the data frame grouped by the zeroth index level. This will
    in turn paginate the number of series in the data frame.
    :param data_frame:
        A data frame to paginate
    :param start:
        The index starting point to slice the data frame at
    :param end:
        The index ending point to slice the data frame at
    :param orders:
        A list of tuples that contain a slicer field definition (with an alias matching the columns of the data frame)
        and a pypika.Order.
    """
    dimension_levels = data_frame.index.names[1:]
    dimension_groups = data_frame.groupby(level=dimension_levels)

    # Do not apply ordering on the 0th dimension !!!
    # This would not have any result since the X-Axis on a chart is ordered sequentially
    orders = [
        (field, orientation)
        for field, orientation in orders
        if alias_selector(field.alias) != data_frame.index.names[0]
    ]

    if orders:
        aggregated_df = dimension_groups.aggregate(_aggregate_dimension_groups)
        sort, ascending = _get_sorting_schema(orders)
        sorted_df = aggregated_df.sort_values(by=sort, ascending=ascending)
        sorted_dimension_values = tuple(sorted_df.index)[start:end]

    else:
        sorted_dimension_values = tuple(dimension_groups.apply(lambda g: g.name))[start:end]

    sorted_dimension_values = (
        pd.Index(sorted_dimension_values, name=dimension_levels[0])
        if len(dimension_levels) == 1
        else pd.MultiIndex.from_tuples(sorted_dimension_values, names=dimension_levels)
    )

    def _apply_pagination(df):
        # This function applies sorting by using the sorted dimension values as an index to select values in the right
        # order out of the data frame. The index must be filtered to only values that are in this data frame, since
        # there might be missing combinations of index values.
        dfx = df.reset_index(level=0, drop=True)
        value_in_index = sorted_dimension_values.isin(dfx.index)
        index_slice = sorted_dimension_values[value_in_index].values

        """
        In the case of bool dimensions, convert index_slice to an array of literal `True`, because pandas `.loc` handles
        lists of bool as a mask.
        """
        if bool in {type(x) for x in sorted_dimension_values}:
            index_slice |= True

        # Need to include nulls so append them to the end of the sorted data frame
        isnull = _index_isnull(dfx)

        return dfx.loc[index_slice, :].append(dfx[isnull])

    return (
        data_frame.sort_values(data_frame.index.names[0], ascending=True)
        .groupby(level=0)
        .apply(_apply_pagination)
    )
