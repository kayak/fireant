from copy import deepcopy

from pypika import Case

from fireant.dataset.modifiers import (
    DimensionModifier,
    ResultSet,
)
from ..utils import (
    alias_selector,
    flatten,
)


def _make_set_dimension(set_filter):
    """
    Returns a dimension that uses a CASE statement as its definition, in order to represent membership to a set,
    given the provided conditional.

    :param set_filter: A ResultSet instance.
    :return: A Field instance.
    """
    old_definition = set_filter.filter.definition
    old_definition_sql = old_definition.get_sql(quote_char="")

    set_dimension = deepcopy(set_filter.filter.field)

    set_term = set_filter.set_label
    complement_term = set_filter.complement_label

    if not set_term and not complement_term:
        set_term = "set({})".format(old_definition_sql)
        complement_term = "complement({})".format(old_definition_sql)

    if not set_filter.will_group_complement:
        complement_term = set_filter.filter.field.definition

    # Only terms wrapped with aggregate functions, such as SUM, will evaluate is_aggregate to True in Pypika.
    # This is a good way to validate that the set dimension, in question, is actually encompassing a metric.
    is_metric = set_dimension.is_aggregate

    if is_metric or not set_filter.will_replace_referenced_dimension:
        # When keeping a referenced dimension, we name the set dimension with a custom alias, so as to have no
        # alias clashes. That prevents issues with rollups/share dimensions, given the original dimension
        # is maintained. Also, metrics need to have the same treatment, given that, unlike dimensions, they are
        # never replaced.
        set_dimension.alias = alias_selector("set({})".format(old_definition_sql))

    set_dimension.label = (
        "Set({})".format(old_definition_sql)
        if not set_filter.set_label
        else set_filter.set_label
    )
    set_dimension.definition = (
        Case().when(old_definition, set_term).else_(complement_term)
    )

    return set_dimension


def _unwrap_field(field):
    if isinstance(field, DimensionModifier):
        return field.dimension

    return field


def _replace_field_if_needed(field, fields_per_set_filter):
    set_filter = fields_per_set_filter.get(_unwrap_field(field))

    if not set_filter:
        return (field,)

    set_dimension = _make_set_dimension(set_filter)

    if isinstance(field, DimensionModifier):
        modified_set_dimension = deepcopy(field)
        modified_set_dimension.dimension = set_dimension
        set_dimension = modified_set_dimension

    if set_filter.will_replace_referenced_dimension:
        return (set_dimension,)
    else:
        return (set_dimension, field)


def apply_set_dimensions(fields, filters):
    """
    A transformed list of Field instances, in case `ResultSet` instances are present among the provided filters.

    :param dimensions: A list of Field instances.
    :param filters: A list of Filter instances.
    :return: A list of Field instances.
    """
    set_filters = [fltr for fltr in filters if isinstance(fltr, ResultSet)]

    if not set_filters:
        return fields

    fields_per_set_filter = {
        set_filter.field: set_filter for set_filter in set_filters
    }
    fields_that_are_not_selected = set(fields_per_set_filter.keys())

    fields_with_set_dimensions = []
    for dimension_or_metric in fields:
        fields_with_set_dimensions.append(
            _replace_field_if_needed(dimension_or_metric, fields_per_set_filter)
        )
        unwrapped_field = _unwrap_field(dimension_or_metric)
        fields_that_are_not_selected.discard(unwrapped_field)
    fields_with_set_dimensions = flatten(fields_with_set_dimensions)

    for dimension_or_metric in fields_that_are_not_selected:
        set_filter = fields_per_set_filter[dimension_or_metric]
        set_dimension = _make_set_dimension(set_filter)
        fields_with_set_dimensions.append(set_dimension)

    return fields_with_set_dimensions


def omit_set_filters(filters):
    """
    Returns all filters but the ones that are `ResultSet` instances.

    :param filters: A list of Filter instances.
    :return: A list of Filter instances.
    """
    return [fltr for fltr in filters if not isinstance(fltr, ResultSet)]
