from typing import Iterable

from fireant.database import Database
from fireant.dataset.fields import Field
from fireant.dataset.filters import Filter
from fireant.dataset.joins import Join
from fireant.dataset.modifiers import Rollup
from fireant.utils import (
    alias_selector,
    flatten,
)
from pypika import (
    Table,
    functions as fn,
)
from .field_helper import (
    make_term_for_dimension,
    make_term_for_metrics,
)
from .finders import (
    find_and_group_references_for_dimensions,
    find_joins_for_tables,
    find_required_tables_to_join,
    find_totals_dimensions,
)
from .reference_helper import adapt_for_reference_query
from .special_cases import apply_special_cases
from .totals_helper import adapt_for_totals_query


@apply_special_cases
def make_slicer_query_with_totals_and_references(database,
                                                 table,
                                                 joins,
                                                 dimensions,
                                                 metrics,
                                                 operations,
                                                 filters,
                                                 references,
                                                 orders,
                                                 share_dimensions=()):
    """
    :param database:
    :param table:
    :param joins:
    :param dimensions:
    :param metrics:
    :param operations:
    :param filters:
    :param references:
    :param orders:
    :param apply_filter_to_totals:
    :param share_dimensions:
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
    fireant.tests.queries.test_build_dimensions.QueryBuilderDimensionTotalsTests
        #test_build_query_with_totals_cat_dimension_with_references
    ```
    """
    totals_dimensions = find_totals_dimensions(dimensions, share_dimensions)
    totals_dimensions_and_none = [None] + totals_dimensions[::-1]

    reference_groups = find_and_group_references_for_dimensions(dimensions, references)
    reference_groups_and_none = [(None, None)] + list(reference_groups.items())

    queries = []
    for totals_dimension in totals_dimensions_and_none:
        (dimensions_for_totals,
         filters_for_totals) = adapt_for_totals_query(totals_dimension,
                                                      dimensions,
                                                      filters)

        for reference_parts, references in reference_groups_and_none:
            (dimensions_for_ref,
             metrics_for_ref,
             filters_for_ref) = adapt_for_reference_query(reference_parts,
                                                          database,
                                                          dimensions_for_totals,
                                                          metrics,
                                                          filters_for_totals,
                                                          references)
            query = make_slicer_query(database,
                                      table,
                                      joins,
                                      dimensions_for_ref,
                                      metrics_for_ref,
                                      filters_for_ref,
                                      orders)

            # Add these to the query instance so when the data frames are joined together, the correct references and
            # totals can be applied when combining the separate result set from each query.
            query._totals = totals_dimension
            query._references = references

            queries.append(query)

    return queries


def make_slicer_query(database: Database,
                      base_table: Table,
                      joins: Iterable[Join] = (),
                      dimensions: Iterable[Field] = (),
                      metrics: Iterable[Field] = (),
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
        dimension_term = make_term_for_dimension(dimension, database.trunc_date)
        query = query.select(dimension_term)
        if not isinstance(dimension, Rollup):
            query = query.groupby(dimension_term)

    # Add filters
    for fltr in filters:
        query = query.having(fltr.definition) \
            if fltr.is_aggregate \
            else query.where(fltr.definition)

    # Add metrics
    metric_terms = [make_term_for_metrics(metric)
                    for metric in metrics]
    if metric_terms:
        query = query.select(*metric_terms)

    # In the case that the orders are determined by a field that is not selected as a metric or dimension, then it needs
    # to be added to the query.
    select_aliases = {el.alias for el in query._selects}
    for (orderby_term, orientation) in orders:
        query = query.orderby(orderby_term, order=orientation)

        if orderby_term.alias not in select_aliases:
            query = query.select(orderby_term)

    return query


def make_latest_query(database: Database,
                      base_table: Table,
                      joins: Iterable[Join] = (),
                      dimensions: Iterable[Field] = ()):
    query = database.query_cls.from_(base_table)

    # Add joins
    join_tables_needed_for_query = find_required_tables_to_join(dimensions, base_table)
    for join in find_joins_for_tables(joins, base_table, join_tables_needed_for_query):
        query = query.join(join.table, how=join.join_type).on(join.criterion)

    for dimension in dimensions:
        f_dimension_key = alias_selector(dimension.alias)
        query = query.select(fn.Max(dimension.definition).as_(f_dimension_key))

    return query


