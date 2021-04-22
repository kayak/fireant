from copy import deepcopy
from typing import List, Tuple, Union

from pypika import (
    Case,
    terms,
)

from fireant.dataset.fields import (
    DataType,
    Field,
    is_metric_field,
)
from fireant.dataset.modifiers import (
    DimensionModifier,
    ResultSet,
)
from ..utils import (
    alias_selector,
    flatten,
)


def _definition_field_for_data_blending(
    target_dataset_definition: Union[Field, terms.Term],
    target_dataset_leaf_definition: terms.Field,
    definition: terms.Term,
) -> terms.Term:
    """
    When using data blending, the dataset table of the set filter needs to be re-mapped to the table in the
    target dataset (i.e. primary or secondary). The easiest way to do that is to select the field in the
    target dataset directly. Otherwise table not found issues would pop up when resolving the joins.

    :param target_dataset_definition: The definition for a field in the target dataset.
    :param target_dataset_leaf_definition: The leaf definition for a field in the target dataset.
                                           Given sometimes a fireant's Field might have nested fireant's
                                           Fields.
    :param definition: A definition that might have its sub-parts (e.g. term, left, right) replaced.
                       That's likely the case for Criterion sub-classes and so on.
    :return: A term sub-class to be used in place of the provided definition argument, when applicable.
    """
    if isinstance(definition, (terms.ValueWrapper,)):
        # Constant values can be returned as is.
        return definition

    if isinstance(definition, (terms.Field,)) and definition == target_dataset_leaf_definition:
        target_dataset_leaf_definition.replace_table(target_dataset_leaf_definition.table, definition.table)
        return target_dataset_definition

    # Function, ...
    if hasattr(definition, 'args'):
        definition.args = [
            _definition_field_for_data_blending(target_dataset_definition, target_dataset_leaf_definition, arg)
            for arg in definition.args
        ]

    # CustomFunction, ...
    if hasattr(definition, 'params'):
        definition.params = [
            _definition_field_for_data_blending(target_dataset_definition, target_dataset_leaf_definition, param)
            for param in definition.params
        ]

    # RangeCriterion, ContainsCriterion, Not, All,
    if hasattr(definition, 'term'):
        definition.term = _definition_field_for_data_blending(
            target_dataset_definition, target_dataset_leaf_definition, definition.term
        )

    # BasicCriterion, ComplexCriterion, ...
    if hasattr(definition, 'left'):
        definition.left = _definition_field_for_data_blending(
            target_dataset_definition, target_dataset_leaf_definition, definition.left
        )

    if hasattr(definition, 'right'):
        definition.right = _definition_field_for_data_blending(
            target_dataset_definition, target_dataset_leaf_definition, definition.right
        )

    # Case
    if hasattr(definition, '_cases'):
        definition._cases = [
            _definition_field_for_data_blending(target_dataset_definition, target_dataset_leaf_definition, case)
            for case in definition._cases
        ]

    if hasattr(definition, '_else'):
        definition._else = _definition_field_for_data_blending(
            target_dataset_definition, target_dataset_leaf_definition, definition._else
        )

    return definition


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

    is_metric = is_metric_field(set_dimension)

    # When using data blending, the dataset table of the set filter needs to be re-mapped to the table in the
    # target dataset (i.e. primary or secondary). Otherwise table not found issues would pop up when resolving
    # the joins.
    if target_dataset and not is_metric:
        target_dataset_definition = target_dataset.fields[set_dimension.alias].definition
        target_dataset_leaf_definition = target_dataset_definition

        while hasattr(target_dataset_leaf_definition, 'definition'):
            # Sometimes a fireant's Field can have nested fireant Fields.
            target_dataset_leaf_definition = target_dataset_leaf_definition.definition

        old_definition = _definition_field_for_data_blending(
            target_dataset_definition, target_dataset_leaf_definition, old_definition
        )

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
    set_dimension.label = "Set({})".format(old_definition_sql) if not set_filter.set_label else set_filter.set_label
    set_dimension.definition = Case().when(old_definition, set_term).else_(complement_term)

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

    if set_filter.will_replace_referenced_dimension and not is_metric_field(set_dimension):
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

    fields_per_set_filter = {set_filter.field: set_filter for set_filter in set_filters}
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
    Returns all filters except the ones that are `ResultSet` instances.

    :param filters: A list of Filter instances.
    :return: A list of Filter instances.
    """
    return [fltr for fltr in filters if not isinstance(fltr, ResultSet)]
