from copy import deepcopy

from pypika import (
    Case,
    terms,
)
from pypika.terms import BasicCriterion
from typing import (
    Tuple,
    List,
)

from fireant.dataset.fields import (
    DataType,
    Field,
)
from fireant.dataset.modifiers import (
    DimensionModifier,
    ResultSet,
)
from ..utils import (
    alias_selector,
    flatten,
)


def _is_metric(field: Field) -> bool:
    """
    Returns whether a field is a metric.

    :param field: A Field instance.
    :return: A boolean.
    """
    # Only terms wrapped with aggregate functions, such as SUM, will evaluate is_aggregate to True in Pypika.
    # This is a good way to validate that the set dimension, in question, is actually encompassing a metric.
    return field.is_aggregate


def _make_set_dimension(set_filter: Field, target_dataset: 'DataSet') -> Field:
    """
    Returns a new dimension that uses a CASE statement as its definition, in order to represent membership to a set,
    given the provided conditional.

    :param set_filter: A ResultSet instance.
    :param target_dataset: A DataSet instance, that will be used as the dataset for which the new dimension
                           will be applied to.
    :return: A new Field instance.
    """
    old_definition = set_filter.filter

    while hasattr(old_definition, 'definition'):
        old_definition = old_definition.definition

    old_definition = deepcopy(old_definition)
    old_definition_sql = old_definition.get_sql(quote_char="")

    set_dimension = deepcopy(set_filter.filter.field)

    is_metric = _is_metric(set_dimension)

    if target_dataset and not is_metric and isinstance(old_definition, BasicCriterion):
        # When using data blending, the dataset table of the set filter needs to be re-mapped to the table in the
        # target dataset (i.e. primary or secondary). The easiest way to do that is to select the field in the
        # target dataset directly.
        target_dataset_definition = deepcopy(target_dataset.fields[set_dimension.alias].definition)

        if not isinstance(old_definition.left, terms.ValueWrapper):
            old_definition.left = target_dataset_definition
        if not isinstance(old_definition.right, terms.ValueWrapper):
            old_definition.right = target_dataset_definition

    set_term = set_filter.set_label
    complement_term = set_filter.complement_label

    if not set_term and not complement_term:
        set_term = "set({})".format(old_definition_sql)
        complement_term = "complement({})".format(old_definition_sql)

    if not set_filter.will_group_complement:
        complement_term = set_filter.filter.field.definition

    if is_metric or not set_filter.will_replace_referenced_dimension:
        # When keeping a referenced dimension, we name the set dimension with a custom alias, so as to have no
        # alias clashes. That prevents issues with rollups/share dimensions, given the original dimension
        # is maintained. Also, metrics need to have the same treatment, given that, unlike dimensions, they are
        # never replaced.
        set_dimension.alias = alias_selector("set({})".format(old_definition_sql))

    set_dimension.data_type = DataType.text
    set_dimension.label = (
        "Set({})".format(old_definition_sql)
        if not set_filter.set_label
        else set_filter.set_label
    )
    set_dimension.definition = (
        Case().when(old_definition, set_term).else_(complement_term)
    )

    # Necessary for set feature to work properly with data blending's field mapping.
    set_dimension.is_artificial = True

    return set_dimension


def _unwrap_field(field: Field) -> Field:
    if isinstance(field, DimensionModifier):
        return field.dimension

    return field


def _replace_field_if_needed(field: Field, fields_per_set_filter, target_dataset: 'DataSet') -> Tuple[Field]:
    set_filter = fields_per_set_filter.get(_unwrap_field(field))

    if not set_filter:
        return (field,)

    set_dimension = _make_set_dimension(set_filter, target_dataset)

    if isinstance(field, DimensionModifier):
        modified_set_dimension = deepcopy(field)
        modified_set_dimension.dimension = set_dimension
        set_dimension = modified_set_dimension

    if set_filter.will_replace_referenced_dimension and not _is_metric(set_dimension):
        # Metrics should not be replaced.
        return (set_dimension,)
    else:
        return (set_dimension, field)


def apply_set_dimensions(fields, filters, target_dataset: 'DataSet') -> List[Field]:
    """
    A transformed list of Field instances, in case `ResultSet` instances are present among the provided filters.

    :param fields: A list of Field instances.
    :param filters: A list of Filter instances.
    :param target_dataset: A DataSet instance.
    :return: A list of Field instances.
    """
    set_filters = [fltr for fltr in filters if isinstance(fltr, ResultSet)]

    if not set_filters:
        return [*fields]

    fields_per_set_filter = {
        set_filter.field: set_filter for set_filter in set_filters
    }
    fields_that_are_not_selected = set(fields_per_set_filter.keys())

    fields_with_set_dimensions = []
    for dimension_or_metric in fields:
        fields_with_set_dimensions.append(
            _replace_field_if_needed(dimension_or_metric, fields_per_set_filter, target_dataset)
        )
        unwrapped_field = _unwrap_field(dimension_or_metric)
        fields_that_are_not_selected.discard(unwrapped_field)
    fields_with_set_dimensions = flatten(fields_with_set_dimensions)

    for dimension_or_metric in fields_that_are_not_selected:
        set_filter = fields_per_set_filter[dimension_or_metric]

        if target_dataset and set_filter.field.alias not in target_dataset.fields:
            continue

        set_dimension = _make_set_dimension(set_filter, target_dataset)
        fields_with_set_dimensions.append(set_dimension)

    return fields_with_set_dimensions


def omit_set_filters(filters):
    """
    Returns all filters but the ones that are `ResultSet` instances.

    :param filters: A list of Filter instances.
    :return: A list of Filter instances.
    """
    return [fltr for fltr in filters if not isinstance(fltr, ResultSet)]
