# coding: utf-8


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


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.

    https://stackoverflow.com/questions/38987/how-to-merge-two-python-dictionaries-in-a-single-expression
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
