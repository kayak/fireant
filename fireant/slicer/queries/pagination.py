import pandas as pd

from pypika import Order


def _get_window(limit, offset):
    start = offset
    end = offset + limit \
        if None not in (offset, limit) \
        else limit
    return start, end


def _apply_sorting(orders):
    sort_values, ascending = zip(*[(order[0].alias, order[1] is Order.asc)
                                   for order in orders])
    return list(sort_values), ascending


def paginate(data_frame, widgets, orders=(), limit=None, offset=None):
    """
    :param data_frame:
        The result set to paginate.
    :param widgets:
        An iterable of widgets that the pagination is being applied for.
    :param orders:
        An iterable of (<Dimension/Metric>, pypika.Order)
    :param limit:
        A limit of the number of data points/series
    :param offset:
        A offset of the number of data points/series
    :return:
        A paginated data frame. If the widget required grouped pagination, then there should be an upperbound
        `limit*(n_index_level_0)`. Otherwise the data frame should have the same length as the limit.
    """
    start, end = _get_window(limit, offset)
    group_pagination = isinstance(data_frame.index, pd.MultiIndex) \
                       and any([getattr(widget, 'group_pagination', False)
                                for widget in widgets])

    if group_pagination:
        return _group_paginate(data_frame, start, end, orders)
    return _simple_paginate(data_frame, start, end, orders)


def _simple_paginate(data_frame, start=None, end=None, orders=()):
    """
    Applies pagination which limits the number of rows in the data frame.

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
        sort, ascending = _apply_sorting(orders)
        data_frame = data_frame.sort_values(by=sort,
                                            ascending=ascending)

    return data_frame[start:end]


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
    orders = [order
              for order in orders
              if order[0].alias != data_frame.index.names[0]]

    if orders:
        # FIXME this should aggregate according to field definition, instead of sum
        # Need a way to interpret definitions in python code in order to do that
        aggregated_df = dimension_groups.sum()

        sort, ascending = _apply_sorting(orders)
        sorted_df = aggregated_df.sort_values(by=sort,
                                              ascending=ascending)
        sorted_dimension_values = tuple(sorted_df.index)[start:end]

    else:
        sorted_dimension_values = tuple(dimension_groups.apply(lambda g: g.name))[start:end]

    sorted_dimension_values = pd.Index(sorted_dimension_values, name=dimension_levels[0]) \
        if len(dimension_levels) == 1 \
        else pd.MultiIndex.from_tuples(sorted_dimension_values, names=dimension_levels)

    def _apply_pagination(df):
        dfx = df.reset_index(level=0, drop=True)
        index_slice = sorted_dimension_values[sorted_dimension_values.isin(dfx.index)]
        return dfx.loc[index_slice, :]

    return data_frame \
        .sort_values(data_frame.index.names[0], ascending=True) \
        .groupby(level=0) \
        .apply(_apply_pagination)
