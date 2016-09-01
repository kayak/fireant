# coding: utf-8
import itertools

import pandas as pd


def dimension_levels(dimension_key, dimension):
    if 'display_field' in dimension:
        return [dimension_key, dimension['display_field']]
    return [dimension_key]


def wrap_list(value):
    return value if isinstance(value, (tuple, list)) else [value]


def slice_first(item):
    if isinstance(item, (tuple, list)):
        return item[0]
    return item


def correct_dimension_level_order(dataframe, display_schema):
    if isinstance(dataframe.index, pd.MultiIndex):
        dimension_orders = [order
                            for key, dimension in display_schema['dimensions'].items()
                            for order in dimension_levels(key, dimension)]

        dataframe = dataframe.reorder_levels(dataframe.index.names.index(level)
                                             for level in dimension_orders)

    metrics = list(display_schema['metrics'])
    if display_schema.get('references'):
        references = [''] + list(display_schema['references'])
        return dataframe[list(itertools.product(references, metrics))]

    return dataframe[metrics]


def filter_duplicates(iterable):
    filtered_list, seen = [], set()
    for item in iterable:
        key = slice_first(item)

        if key in seen:
            continue

        seen.add(key)
        filtered_list.append(item)

    return filtered_list
