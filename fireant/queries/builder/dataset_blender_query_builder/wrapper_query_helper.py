import copy

from fireant.dataset.modifiers import DimensionModifier
from fireant.queries.finders import find_metrics_for_widgets
from fireant.utils import alias_selector


def _make_metrics_for_wrapper_query(widgets, sub_query_per_table):
    """
    Copies and replaces the table from metric's definition with the subquery, that is supposed to represent the
    provided table.

    :param widgets:
        A list of Widget instances.
    :param sub_query_per_table:
        A dictionary with tables as keys and their respective subqueries as values.
    :return: A list of Metric instances to be used by the wrapper query.
    """
    metrics_for_wrapper_query = [copy.deepcopy(metric) for metric in find_metrics_for_widgets(widgets)]

    for metric in metrics_for_wrapper_query:
        new_definition = copy.deepcopy(metric.definition)

        for pypika_field in new_definition.fields():
            sub_query = sub_query_per_table[pypika_field.table]
            pypika_field.table = sub_query
            if len(new_definition.fields()) == 1 and not pypika_field.name.startswith('$'):
                pypika_field.name = alias_selector(metric.alias)

        metric.definition = new_definition

    return metrics_for_wrapper_query


def _make_filters_for_wrapper_query(metrics, filters, sub_query_per_table):
    """
    Copies and replaces the table from filter's definition with the subquery, that is supposed to represent the
    provided table.

    :param metrics:
        A list of Metric instances.
    :param filters:
        A list of Filter instances.
    :param sub_query_per_table:
        A dictionary with tables as keys and their respective subqueries as values.
    :return: A list of Filter instances to be used by the wrapper query.
    """
    # Filtering dimensions is not necessary since mapping dimensions are present in all datasets
    filters_for_metrics_only = []

    metric_names_from_blender_dataset = set(metric.alias for metric in metrics)

    # WARNING: Filters need to be deep copied, since they are raw pypika instances changed in place
    for filter in [copy.deepcopy(filter) for filter in filters]:
        # We only filter in the blended query when it is a metric filter, otherwise a filtering metrics in a
        # secondary dataset would still return filtered rows
        if filter.field_alias not in metric_names_from_blender_dataset:
            continue

        new_definition = copy.deepcopy(filter.definition)
        for pypika_field in new_definition.fields():
            sub_query = sub_query_per_table[pypika_field.table]
            pypika_field.table = sub_query
            if len(filter.definition.fields()) == 1 and not pypika_field.name.startswith('$'):
                pypika_field.name = alias_selector(filter.field_alias)
        filter.definition = new_definition

        filters_for_metrics_only.append(filter)

    return filters_for_metrics_only


def _make_dimensions_for_wrapper_query(dimensions, sub_query_per_table):
    """
    Copies and replaces the table from dimension's definition with the subquery, that is supposed to represent the
    provided table. Dimension modifiers are also unwrapped since those will be applied in the nested query level.

    :param dimensions:
        A list of Dimension instances.
    :param sub_query_per_table:
        A dictionary with tables as keys and their respective subqueries as values.
    :return: A list of Dimension instances to be used by the wrapper query.
    """
    # Unwrap dimension modifiers on wrapper query level, since modifiers were already applied on sub-queries
    dimensions_without_modifiers = []

    for dimension in [copy.deepcopy(dimension) for dimension in dimensions]:
        if isinstance(dimension, DimensionModifier):
            # We only need to apply dimension modifiers in the from and join levels
            dimension = dimension.dimension

        sub_query_per_table[dimension._pypika_field_for_referencing_sub_queries.table] = sub_query_per_table[
            dimension.definition.table
        ]
        sub_query = sub_query_per_table[dimension.definition.table]
        new_definition = copy.deepcopy(dimension.definition)
        new_definition.table = sub_query
        dimension.definition = new_definition

        dimension.alias = alias_selector(dimension.alias)
        dimension.definition.name = alias_selector(dimension.alias)
        dimensions_without_modifiers.append(dimension)

    return dimensions_without_modifiers


def _make_references_for_wrapper_query(references):
    """
    Copies and replaces the table from reference's field definition with the subquery, that is supposed to represent
    the provided table.

    :param references:
        A list of Reference instances.
    :param sub_query_per_table:
        A dictionary with tables as keys and their respective subqueries as values.
    :return: A list of Reference instances to be used by the wrapper query.
    """
    references_for_wrapper_query = []

    for reference in [copy.deepcopy(reference) for reference in references]:
        dimension = copy.deepcopy(reference.field)
        reference.field = dimension

        if isinstance(dimension, DimensionModifier):
            # We only need to apply dimension modifiers in the from and join levels
            dimension = dimension.dimension

        dimension.definition = copy.deepcopy(dimension.definition)
        dimension.alias = alias_selector(dimension.alias) + '_' + reference.alias
        dimension.definition.name = alias_selector(dimension.alias) + '_' + reference.alias
        references_for_wrapper_query.append(reference)

    return references_for_wrapper_query
