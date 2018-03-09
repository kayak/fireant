from collections import OrderedDict


def wrap_list(value):
    return value if isinstance(value, (tuple, list)) else [value]


def deep_get(d, keys, default=None):
    d_level = d
    for key in keys:
        if key not in d_level:
            return default
        d_level = d_level[key]
    return d_level

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


def immutable(func):
    """
    Decorator for wrapper "builder" functions.  These are functions on the Query class or other classes used for
    building queries which mutate the query and return self.  To make the build functions immutable, this decorator is
    used which will deepcopy the current instance.  This decorator will return the return value of the inner function
    or the new copy of the instance.  The inner function does not need to return self.
    """
    import copy

    def _copy(self, *args, **kwargs):
        self_copy = copy.deepcopy(self)
        result = func(self_copy, *args, **kwargs)

        # Return self if the inner function returns None.  This way the inner function can return something
        # different (for example when creating joins, a different builder is returned).
        if result is None:
            return self_copy

        return result

    return _copy


def ordered_distinct_list(l):
    seen = set()
    return [x
            for x in l
            if not x in seen
            and not seen.add(x)]


def ordered_distinct_list_by_attr(l, attr='key'):
    seen = set()
    return [x
            for x in l
            if not getattr(x, attr) in seen
            and not seen.add(getattr(x, attr))]


def groupby(items, by):
    """
    Group items using a function to derive a key.
    :param items: The items to group
    :param by: A lambda function to create a key based on the item
    :return:
        an Ordered dict
    """

    result = OrderedDict()
    for item in items:
        key = by(item)

        if key in result:
            result[key].append(item)
        else:
            result[key] = [item]

    return result


def groupby_first_level(index):
    seen = set()
    return [x[1:]
            for x in list(index)
            if x[1:] not in seen and not seen.add(x[1:])]
