# coding: utf-8


def df_correct_level_order(dataframe, display_schema):
    dimension_orders = [order
                        for key, dimension in display_schema['dimensions'].items()
                        for order in [key] + ([dimension['display_field']]
                                              if 'display_field' in dimension
                                              else [])]

    reordered = dataframe.reorder_levels(dataframe.index.names.index(level)
                                         for level in dimension_orders)
    return reordered
