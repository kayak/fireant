from typing import Iterable

from fireant.utils import (
    flatten,
    format_dimension_key,
    format_metric_key,
)
from pypika import (
    Table,
    functions as fn,
)
from .finders import (
    find_and_group_references_for_dimensions,
    find_joins_for_tables,
    find_required_tables_to_join,
    find_rolled_up_dimensions,
)
from .references import adapt_for_reference_query
from .special_cases import apply_special_cases
from ..dimensions import (
    Dimension,
    TotalsDimension,
)
from ..filters import (
    DimensionFilter,
    Filter,
)
from ..joins import Join
from ..metrics import Metric
from ...database import Database


def adapt_for_totals_query(totals_dimension, dimensions):
    # Get an index to split the dimensions before and after the totals dimension
    index = dimensions.index(totals_dimension) \
        if totals_dimension is not None \
        else len(dimensions)

    grouped_dims, totaled_dims = dimensions[:index], dimensions[index:]
    return grouped_dims + [TotalsDimension(dimension)
                           for dimension in totaled_dims]


@apply_special_cases
def make_slicer_query_with_rollup_and_references(database,
                                                 table,
                                                 joins,
                                                 dimensions,
                                                 metrics,
                                                 operations,
                                                 filters,
                                                 references,
                                                 orders):
    """
    :param database:
    :param table:
    :param joins:
    :param dimensions:
    :param metrics:
    :param filters:
    :param orders:
    :param references:
    :param operations:
    :return:
    """

    """
    The following two loops will run over the spread of the two sets including a NULL value in each set:
     - reference group (WoW, MoM, etc.)
     - dimension with roll up/totals enabled (totals dimension)

    This will result in at least one query where the reference group and totals dimension is NULL, which shall be
    called base query. The base query will ALWAYS be present, even if there are zero reference groups or totals
    dimensions.

    For a concrete example, check the test case in :
    ```
    fireant.tests.slicer.queries.test_build_dimensions.QueryBuilderDimensionTotalsTests
        #test_build_query_with_totals_cat_dimension_with_references
    ```
    """
    rollup_dimensions = find_rolled_up_dimensions(dimensions)
    rollup_dimensions_and_none = [None] + rollup_dimensions

    reference_groups = find_and_group_references_for_dimensions(references)
    reference_groups_and_none = [(None, None)] + list(reference_groups.items())

    queries = []
    for rollup_dimension in rollup_dimensions_and_none:
        query_dimensions = adapt_for_totals_query(rollup_dimension, dimensions)

        for reference_parts, references in reference_groups_and_none:
            (ref_database,
             ref_dimensions,
             ref_metrics,
             ref_filters) = adapt_for_reference_query(reference_parts,
                                                      database,
                                                      query_dimensions,
                                                      metrics,
                                                      filters,
                                                      references)
            query = make_slicer_query(ref_database,
                                      table,
                                      joins,
                                      ref_dimensions,
                                      ref_metrics,
                                      ref_filters,
                                      orders)

            # Add this to the query instance so when the data frames are joined together, the correct references can be
            # applied.
            query._references = references

            queries.append(query)

    return queries


def make_slicer_query(database: Database,
                      base_table: Table,
                      joins: Iterable[Join] = (),
                      dimensions: Iterable[Dimension] = (),
                      metrics: Iterable[Metric] = (),
                      filters: Iterable[Filter] = (),
                      orders: Iterable = ()):
    """
    Creates a pypika/SQL query from a list of slicer elements.

    This is the base implementation shared by two implementations: the query to fetch data for a slicer request and
    the query to fetch choices for dimensions.

    This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
    for the query to fetch choices for dimensions.

    The slicer query extends this with metrics, references, and totals.

    :param database:

    :param base_table:
        pypika.Table - The base table of the query, the one in the FROM clause
    :param joins:
        A collection of joins available in the slicer. This should include all slicer joins. Only joins required for
        the query will be used.
    :param dimensions:
        A collection of dimensions to use in the query.
    :param metrics:
        A collection of metircs to use in the query.
    :param filters:
        A collection of filters to apply to the query.
    :param orders:
        A collection of orders as tuples of the metric/dimension to order by and the direction to order in.
    :return:
    """
    query = database.query_cls.from_(base_table)

    elements = flatten([metrics, dimensions, filters])

    # Add joins
    join_tables_needed_for_query = find_required_tables_to_join(elements, base_table)
    for join in find_joins_for_tables(joins, base_table, join_tables_needed_for_query):
        query = query.join(join.table, how=join.join_type).on(join.criterion)

    # Add dimensions
    for dimension in dimensions:
        terms = make_terms_for_dimension(dimension, database.trunc_date)
        query = query.select(*terms)
        # Don't group TotalsDimensions
        if not isinstance(dimension, TotalsDimension):
            query = query.groupby(*terms)

    # Add filters
    for filter_ in filters:
        query = query.where(filter_.definition) \
            if isinstance(filter_, DimensionFilter) \
            else query.having(filter_.definition)

    # Add metrics
    terms = make_terms_for_metrics(metrics)
    if terms:
        query = query.select(*terms)

    select_aliases = {el.alias for el in query._selects}
    for (term, orientation) in orders:
        query = query.orderby(term, order=orientation)

        if term.alias not in select_aliases:
            query = query.select(term)

    return query


def make_latest_query(database: Database,
                      base_table: Table,
                      joins: Iterable[Join] = (),
                      dimensions: Iterable[Dimension] = ()):
    query = database.query_cls.from_(base_table)

    # Add joins
    join_tables_needed_for_query = find_required_tables_to_join(dimensions, base_table)
    for join in find_joins_for_tables(joins, base_table, join_tables_needed_for_query):
        query = query.join(join.table, how=join.join_type).on(join.criterion)

    for dimension in dimensions:
        f_dimension_key = format_dimension_key(dimension.key)
        query = query.select(fn.Max(dimension.definition).as_(f_dimension_key))

    return query


def make_terms_for_metrics(metrics):
    return [metric.definition.as_(format_metric_key(metric.key))
            for metric in metrics]


def make_terms_for_dimension(dimension, window=None):
    """
    Makes a list of pypika terms for a given slicer definition.

    :param dimension:
        A slicer dimension.
    :param window:
        A window function to apply to the dimension definition if it is a continuous dimension.
    :return:
        a list of terms required to select and group by in a SQL query given a slicer dimension. This list will contain
        either one or two elements. A second element will be included if the dimension has a definition for its display
        field.
    """

    # Apply the window function to continuous dimensions only
    dimension_definition = (
        window(dimension.definition, dimension.interval)
        if window and hasattr(dimension, 'interval')
        else dimension.definition
    ).as_(format_dimension_key(dimension.key))

    # Include the display definition if there is one
    return [
        dimension_definition,
        dimension.display_definition.as_(format_dimension_key(dimension.display_key))
    ] if dimension.has_display_field else [
        dimension_definition
    ]


def make_orders_for_dimensions(dimensions):
    """
    Creates a list of ordering for a slicer query based on a list of dimensions. The dimensions's display definition is
    used preferably as the ordering term but the definition is used for dimensions that do not have a display
    definition.

    :param dimensions:
    :return:
        a list of tuple pairs like (term, orientation) for ordering a SQL query where the first element is the term
        to order by and the second is the orientation of the ordering, ASC or DESC.
    """

    # Use the same function to make the definition terms to force it to be consistent.
    # Always take the last element in order to prefer the display definition.
    definitions = [make_terms_for_dimension(dimension)[-1]
                   for dimension in dimensions]

    return [(definition, None)
            for definition in definitions]
