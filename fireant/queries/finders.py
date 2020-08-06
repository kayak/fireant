from collections import defaultdict, namedtuple
from typing import TYPE_CHECKING, Union

from pypika.terms import Field as PyPikaField
from toposort import CircularDependencyError, toposort_flatten

from fireant.dataset.filters import Filter
from fireant.dataset.intervals import DATETIME_INTERVALS, DatetimeInterval
from fireant.dataset.modifiers import OmitFromRollup, Rollup
from fireant.dataset.operations import Share
from fireant.exceptions import DataSetException
from fireant.utils import groupby, ordered_distinct_list, ordered_distinct_list_by_attr

if TYPE_CHECKING:
    from fireant import Field

class MissingTableJoinException(DataSetException):
    pass


class CircularJoinsException(DataSetException):
    pass


ReferenceGroup = namedtuple("ReferenceGroup", ("dimension", "time_unit", "intervals"))


def _get_field_definition(field: Union['Field', 'Filter']) -> PyPikaField:
    if field.is_wrapped:
        return getattr(field.definition, 'definition', None)

    return getattr(field, 'definition', None)


def find_required_tables_to_join(elements, base_table):
    """
    Collect all the tables required for a given list of dataset elements.  This looks through the definition and
    display_definition attributes of all elements and

    This looks through the metrics, dimensions, and filter included in this dataset query. It also checks both the
    definition
    field of each element as well as the display definition for Unique Dimensions.

    :return:
        A collection of tables required to execute a query,
    """
    return ordered_distinct_list(
        [
            table
            for element in elements
            # Need extra for-loop to incl. the `display_definition` from `UniqueDimension`
            for attr in [_get_field_definition(element)]
            # ... but then filter Nones since most elements do not have `display_definition`
            if attr is not None
            for table in attr.tables_
            # Omit the base table from this list
            if base_table != table
        ]
    )


def find_joins_for_tables(joins, base_table, required_tables):
    """
    Given a set of tables required for a dataset query, this function finds the joins required for the query and
    sorts them topologically.

    :return:
        A list of joins in the order that they must be joined to the query.
    :raises:
        MissingTableJoinException - If a table is required but there is no join for that table
        CircularJoinsException - If there is a circular dependency between two or more joins
    """
    dependencies = defaultdict(set)
    slicer_joins = {join.table: join for join in joins}

    while required_tables:
        table = required_tables.pop()

        if table not in slicer_joins:
            raise MissingTableJoinException(
                "Could not find a join for table {}".format(str(table))
            )

        join = slicer_joins[table]
        tables_required_for_join = set(join.criterion.tables_) - {
            base_table,
            join.table,
        }

        dependencies[join] |= {
            slicer_joins[table] for table in tables_required_for_join
        }
        required_tables += tables_required_for_join - {d.table for d in dependencies}

    try:
        return toposort_flatten(dependencies, sort=True)
    except CircularDependencyError as e:
        raise CircularJoinsException(str(e))


def find_metrics_for_widgets(widgets):
    """
    :return:
        an ordered, distinct list of metrics used in all widgets as part of this query.
    """
    return ordered_distinct_list_by_attr(
        [metric for widget in widgets for metric in widget.metrics]
    )


def find_operations_for_widgets(widgets):
    """
    :return:
        an ordered, distinct list of metrics used in all widgets as part of this query.
    """
    return ordered_distinct_list_by_attr(
        [operation for widget in widgets for operation in widget.operations]
    )


def find_dataset_fields(metrics):
    """
    Given a list of metrics used in widgets from a dataset blender query, this function returns a list of metrics that
    are from a dataset. Concretely, this means that if a dataset blender has a metric built on dataset metrics, then
    this will replace that metric with the metrics from the dataset.
    """
    from fireant.dataset.fields import Field

    return [
        field or metric
        for metric in metrics
        for field in metric.definition.find_(Field)
    ]


def find_share_operations(operations):
    return [operation for operation in operations if isinstance(operation, Share)]


def find_share_dimensions(dimensions, operations):
    """
    Returns a subset list of dimensions from the list of dimensions that are used as the over-dimension in share
    operations.

    :param dimensions:
    :param operations:
    :return:
    """
    dimension_map = {dimension.alias: dimension for dimension in dimensions}

    return [
        dimension_map[operation.over.alias]
        for operation in operations
        if isinstance(operation, Share)
        and operation.over is not None
        and operation.over.alias in dimension_map
    ]


def find_totals_dimensions(dimensions, share_dimensions):
    """
    :param dimensions:
    :param share_dimensions:
    :return:
        an list of all dimension field in the list argument `dimensions` which have the `Rollup` modifier applied to
        them or are used as a basis for a share metric.
    """
    share_dimension_aliases = {d.alias for d in share_dimensions}
    return [
        dimension
        for dimension in dimensions
        if isinstance(dimension, Rollup) or dimension.alias in share_dimension_aliases
    ]


def find_filters_for_totals(filters):
    """
    :param filters:
    :return:
        a list of filters that should be applied to totals queries. This removes any filters from the list that have
        the `OmitFromRollup` modifier applied to them.
    """
    return [fltr for fltr in filters if not isinstance(fltr, OmitFromRollup)]


def find_field_in_modified_field(field):
    """
    Returns the field from a modified field argument (or just the field argument if it is not modified). A modified
    field represents either a wrapped dimension (e.g. DatetimeInterval) or a field wrapped in a filter.
    """
    modified_field = field
    while hasattr(modified_field, "dimension"):
        modified_field = modified_field.dimension
    while hasattr(modified_field, "field"):
        modified_field = modified_field.field
    return modified_field


interval_weekdays = {
    "month": ("week", 4),
    "quarter": ("week", 4 * 3),
    "year": ("week", 4 * 13),
}


def find_and_group_references_for_dimensions(dimensions, references):
    """
    Finds all of the references for dimensions and groups them by dimension, interval unit, number of intervals.

    This structure reflects how the references need to be joined to the dataset query. References of the same
    type (WoW, WoW.delta, WoW.delta_percent) can share a join query.

    :param dimensions:
    :param references:

    :return:
        An `OrderedDict` where the keys are 3-item tuples consisting of "Dimension, interval unit, # of intervals.

        .. code-block:: python

            Example
            {
                (Dimension(date_1), 'weeks', 1): [WoW, WoW.delta],
                (Dimension(date_1), 'years', 1): [YoY],
                (Dimension(date_7), 'days', 1): [DoD, DoD.delta_percent],
            }
    """
    align_weekdays = (
        dimensions
        and isinstance(dimensions[0], DatetimeInterval)
        and -1 < DATETIME_INTERVALS.index(dimensions[0].interval_key) < 3
    )

    def get_dimension_time_unit_and_interval(reference):
        defaults = (reference.time_unit, 1)
        time_unit, interval_muliplier = (
            interval_weekdays.get(reference.time_unit, defaults)
            if align_weekdays
            else defaults
        )

        field = find_field_in_modified_field(reference.field)
        return field, time_unit, interval_muliplier * reference.interval

    distinct_references = ordered_distinct_list(references)
    return groupby(distinct_references, get_dimension_time_unit_and_interval)
