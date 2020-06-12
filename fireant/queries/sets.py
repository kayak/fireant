from copy import deepcopy

from pypika import Case

from .finders import find_filters_for_sets
from ..utils import (
    alias_selector,
    flatten,
)


def _make_set_dimension(set_filter):
    """
    Returns a dimension that uses a CASE statement as its definition, in order to represent membership to a set,
    given the provided conditional.

    :param set_filter:
    :return:
    """
    old_definition = set_filter.filter.definition
    old_definition_sql = old_definition.get_sql(quote_char="")

    new_dimension = deepcopy(set_filter.filter.field)
    has_no_in_and_out_labels_sets = not set_filter.set_label and not set_filter.complement_label

    new_dimension.alias = alias_selector('set({})'.format(old_definition_sql))
    new_dimension.label = 'Set({})'.format(old_definition_sql) if has_no_in_and_out_labels_sets else set_filter.set_label
    new_dimension.definition = Case().when(
        old_definition, "set({})".format(old_definition_sql) if has_no_in_and_out_labels_sets else set_filter.set_label
    ).else_("complement({})".format(old_definition_sql) if has_no_in_and_out_labels_sets else set_filter.complement_label)

    return new_dimension


def _replace_dimension_if_needed(dimension, dimensions_per_set_filter):
    set_filter = dimensions_per_set_filter.get(dimension)

    if not set_filter:
        return (dimension,)

    set_dimension = _make_set_dimension(set_filter)

    if set_filter.will_ignore_dimensions:
        return (set_dimension,)
    else:
        return (set_dimension, dimension)


def adapt_for_sets_query(dimensions, orders, filters):
    """
    Adapt filters for sets query. This function will select filters with `ResultSet` modifier. A `ResultSet` modifier
    operates as a special kind of filter, which won't actually filter the data. Instead it will create
    dimensions so as to represent membership to a set, given the provided conditional. If will_ignore_dimensions
    kwarg is False, which is the case by default, it will replace the dimension used in the conditional, if selected.

    :param dimensions:
    :param orders:
    :param filters:
    :return:
    """
    set_filters = find_filters_for_sets(filters)

    if not set_filters:
        return dimensions, orders, filters

    dimensions_per_set_filter = {
        set_filter.field: set_filter
        for set_filter in set_filters
    }
    dimensions_that_are_not_selected = set(dimensions_per_set_filter.keys())

    current_dimensions = []
    for dimension in dimensions:
        current_dimensions.append(_replace_dimension_if_needed(dimension, dimensions_per_set_filter))
        dimensions_that_are_not_selected.discard(dimension)
    current_dimensions = flatten(current_dimensions)

    current_orders = []
    for (dimension, ordering) in orders:
        current_orders.append([(dim, ordering) for dim in _replace_dimension_if_needed(dimension, dimensions_per_set_filter)])
    current_orders = flatten(current_orders)

    for dimension in dimensions_that_are_not_selected:
        set_filter = dimensions_per_set_filter[dimension]
        new_dimension = _make_set_dimension(set_filter)
        current_dimensions.append(new_dimension)
        current_orders.append((new_dimension, None))

    other_filters = [fltr for fltr in filters if fltr not in set_filters]

    return current_dimensions, current_orders, other_filters
