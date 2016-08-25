# coding: utf-8


def dimension_levels(dimension_key, dimension):
    if 'display_field' in dimension:
        return [dimension_key, dimension['display_field']]
    return [dimension_key]


def correct_dimension_level_order(dataframe, display_schema):
    dimension_orders = [order
                        for key, dimension in display_schema['dimensions'].items()
                        for order in dimension_levels(key, dimension)]

    reordered = dataframe.reorder_levels(dataframe.index.names.index(level)
                                         for level in dimension_orders)
    return reordered
