from collections import OrderedDict


def wrap_list(value):
    return value if isinstance(value, (tuple, list)) else [value]


def setdeepattr(d, keys, value):
    """
    Similar to the built-in `setattr`, this function accepts a list/tuple of keys to set a value deep in a `dict`

    Given the following dict structure

    .. code-block:: python

        d = {
          'A': {
            '0': {
              'a': 1,
              'b': 2,
            }
          },
        }

    Calling `setdeepattr` with a key path to a value deep in the structure will set that value. If the value or any
    of the objects in the key path do not exist, then a dict will be created.

    .. code-block:: python

        # Overwrites the value in `d` at A.0.a, which was 1, to 3
        setdeepattr(d, ('A', '0', 'a'), 3)

        # Adds an entry in `d` to A.0 with the key 'c' and the value 3
        setdeepattr(d, ('A', '0', 'c'), 3)

        # Adds an entry in `d` with the key 'X' and the value a new dict
        # Adds an entry in `d` to `X` with the key '0' and the value a new dict
        # Adds an entry in `d` to `X.0` with the key 'a' and the value 0
        setdeepattr(d, ('X', '0', 'a'), 0)

    :param d:
        A dict value with nested dict attributes.
    :param keys:
        A list/tuple path of keys in `d` to the desired value
    :param value:
        The value to set at the given path `keys`.
    """
    if not isinstance(keys, (list, tuple)):
        keys = (keys,)

    top, *rest = keys

    if rest:
        if top not in d:
            d[top] = {}

        setdeepattr(d[top], rest, value)

    else:
        d[top] = value


def getdeepattr(d, keys, default_value=None):
    """
    Similar to the built-in `getattr`, this function accepts a list/tuple of keys to get a value deep in a `dict`

    Given the following dict structure

    .. code-block:: python

        d = {
          'A': {
            '0': {
              'a': 1,
              'b': 2,
            }
          },
        }

    Calling `getdeepattr` with a key path to a value deep in the structure will return that value. If the value or any
    of the objects in the key path do not exist, then the default value is returned.

    .. code-block:: python

        assert 1 == getdeepattr(d, ('A', '0', 'a'))
        assert 2 == getdeepattr(d, ('A', '0', 'b'))
        assert 0 == getdeepattr(d, ('A', '0', 'c'), default_value=0)
        assert 0 == getdeepattr(d, ('X', '0', 'a'), default_value=0)

    :param d:
        A dict value with nested dict attributes.
    :param keys:
        A list/tuple path of keys in `d` to the desired value
    :param default_value:
        A default value that will be returned if the path `keys` does not yield a value.
    :return:
        The value following the path `keys` or `default_value`
    """
    d_level = d

    for key in keys:
        if key not in d_level:
            return default_value

        d_level = d_level[key]

    return d_level


def flatten(items):
    return [item for level in items for item in wrap_list(level)]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    return [l[i:i + n]
            for i in range(0, len(l), n)]


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

    :param items:
        The items to group

    :param by:
        A lambda function to create a key based on the item

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


def format_key(key, prefix=None):
    if key is None:
        return key

    if prefix is not None:
        return '${}${}'.format(prefix, key)

    return '${}'.format(key)


def format_dimension_key(key):
    return format_key(key, 'd')


def format_metric_key(key):
    return format_key(key, 'm')


def repr_field_key(key):
    field_type_symbol = key[1]
    field_key = key[3:]
    field_type = {'m': 'metrics', 'd': 'dimensions'}[field_type_symbol]
    return 'slicer.{}.{}'.format(field_type, field_key)
