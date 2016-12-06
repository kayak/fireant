# coding: utf-8


def dimension_levels(dimension_key, dimension):
    if 'display_field' in dimension:
        return [dimension_key, dimension['display_field']]
    return [dimension_key]


def wrap_list(value):
    return value if isinstance(value, (tuple, list)) else [value]


def flatten(items):
    return [item for level in items for item in wrap_list(level)]


def slice_first(item):
    if isinstance(item, (tuple, list)):
        return item[0]
    return item


def filter_duplicates(iterable):
    filtered_list, seen = [], set()
    for item in iterable:
        key = slice_first(item)

        if key in seen:
            continue

        seen.add(key)
        filtered_list.append(item)

    return filtered_list
