import copy
from collections import (
    OrderedDict,
    defaultdict,
    namedtuple,
)

from toposort import (
    CircularDependencyError,
    toposort_flatten,
)

from fireant.utils import (
    groupby,
    ordered_distinct_list,
    ordered_distinct_list_by_attr,
)
from ..exceptions import (
    CircularJoinsException,
    MissingTableJoinException,
)

ReferenceGroup = namedtuple('ReferenceGroup', ('dimension', 'time_unit', 'intervals'))


def find_required_tables_to_join(elements, base_table):
    """
    Collect all the tables required for a given list of slicer elements.  This looks through the definition and
    display_definition attributes of all elements and

    This looks through the metrics, dimensions, and filter included in this slicer query. It also checks both the
    definition
    field of each element as well as the display definition for Unique Dimensions.

    :return:
        A collection of tables required to execute a query,
    """
    return ordered_distinct_list([table
                                  for element in elements

                                  # Need extra for-loop to incl. the `display_definition` from `UniqueDimension`
                                  for attr in [getattr(element, 'definition', None),
                                               getattr(element, 'display_definition', None)]

                                  # ... but then filter Nones since most elements do not have `display_definition`
                                  if attr is not None

                                  for table in attr.tables_

                                  # Omit the base table from this list
                                  if base_table != table])


def find_joins_for_tables(joins, base_table, required_tables):
    """
    Given a set of tables required for a slicer query, this function finds the joins required for the query and
    sorts them topologically.

    :return:
        A list of joins in the order that they must be joined to the query.
    :raises:
        MissingTableJoinException - If a table is required but there is no join for that table
        CircularJoinsException - If there is a circular dependency between two or more joins
    """
    dependencies = defaultdict(set)
    slicer_joins = {join.table: join
                    for join in joins}

    while required_tables:
        table = required_tables.pop()

        if table not in slicer_joins:
            raise MissingTableJoinException('Could not find a join for table {}'
                                            .format(str(table)))

        join = slicer_joins[table]
        tables_required_for_join = set(join.criterion.tables_) - {base_table, join.table}

        dependencies[join] |= {slicer_joins[table]
                               for table in tables_required_for_join}
        required_tables += tables_required_for_join - {d.table for d in dependencies}

    try:
        return toposort_flatten(dependencies)
    except CircularDependencyError as e:
        raise CircularJoinsException(str(e))


def find_metrics_for_widgets(widgets):
    """
    :return:
        an ordered, distinct list of metrics used in all widgets as part of this query.
    """
    return ordered_distinct_list_by_attr([metric
                                          for widget in widgets
                                          for metric in widget.metrics])


def find_operations_for_widgets(widgets):
    """
    :return:
        an ordered, distinct list of metrics used in all widgets as part of this query.
    """
    return ordered_distinct_list_by_attr([operation
                                          for widget in widgets
                                          for operation in widget.operations])


def find_dimensions_with_totals(dimensions):
    return [dimension
            for dimension in dimensions
            if dimension.is_rollup]


def find_and_replace_reference_dimensions(references, dimensions):
    """
    Finds the dimension for a reference in the query if there is one and replaces it. This is to force the reference to
    use the same modifiers with a dimension if it is selected in the query.

    :param references:
    :param dimensions:
    :return:
    """
    dimensions_by_key = {dimension.key: dimension
                         for dimension in dimensions}

    reference_copies = []
    for reference in map(copy.deepcopy, references):
        dimension = dimensions_by_key.get(reference.dimension.key)
        if dimension is not None:
            reference.dimension = dimension
        reference_copies.append(reference)
    return reference_copies


def find_and_group_references_for_dimensions(references):
    """
    Finds all of the references for dimensions and groups them by dimension, interval unit, number of intervals.

    This structure reflects how the references need to be joined to the slicer query. References of the same
    type (WoW, WoW.delta, WoW.delta_percent) can share a join query.

    :param references:
    :return:
        An `OrderedDict` where the keys are 3-item tuples consisting of "Dimension, interval unit, # of intervals.

        for example:

        {
            (Dimension(date_1), 'weeks', 1): [WoW, WoW.delta],
            (Dimension(date_1), 'years', 1): [YoY],
            (Dimension(date_7), 'days', 1): [DoD, DoD.delta_percent],
        }
    """

    def get_dimension_time_unit_and_interval(reference):
        return reference.dimension, reference.time_unit, reference.interval

    distinct_references = ordered_distinct_list(references)
    return groupby(distinct_references, get_dimension_time_unit_and_interval)
