from functools import partial

from fireant.utils import flatten
from pypika import JoinType
from .finders import (
    find_joins_for_tables,
    find_required_tables_to_join,
)
from .references import (
    make_dimension_terms_for_reference_container_query,
    make_metric_terms_for_reference_container_query,
    make_reference_filters,
    make_reference_join_criterion,
    make_terms_for_references,
    monkey_patch_align_weekdays,
)
from ..dimensions import TotalsDimension
from ..filters import DimensionFilter
from ..intervals import weekly
from ..references import YearOverYear


def make_slicer_query(database, base_table, joins=(), dimensions=(), metrics=(), filters=()):
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
        Iterable<fireant.Join> - A collection of joins available in the slicer. This should include all slicer joins.
        Only joins required for the query will be used.
    :param dimensions:
        Iterable<fireant.Dimension> - A collection of dimensions to use in the query.
    :param metrics:
        Iterable<fireant.Metric> - A collection of metircs to use in the query.
    :param filters:
        Iterable<fireant.Filter> - A collection of filters to apply to the query.
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

    return query


def make_slicer_query_with_references_and_totals(database, table, joins, dimensions, metrics, filters,
                                                 reference_groups, totals_dimensions):
    """
    WRITEME

    :param database:
    :param table:
    :param joins:
    :param dimensions:
    :param metrics:
    :param filters:
    :param reference_groups:
    :param totals_dimensions:
    :return:
    """
    make_query = partial(make_slicer_query_with_references, reference_groups=reference_groups) \
        if reference_groups else \
        make_slicer_query

    query = make_query(database,
                       table,
                       joins,
                       dimensions,
                       metrics,
                       filters)

    for dimension in totals_dimensions:
        index = dimensions.index(dimension)
        grouped_dims, totaled_dims = dimensions[:index], dimensions[index:]

        # Replace the dimension and all following dimensions with a TotalsDimension. This will prevent those dimensions
        # from being grouped on in the totals UNION query, selecting NULL instead
        dimensions_with_totals = grouped_dims + [TotalsDimension(dimension)
                                                 for dimension in totaled_dims]

        totals_query = make_query(database,
                                  table,
                                  joins,
                                  dimensions_with_totals,
                                  metrics,
                                  filters)

        # UNION ALL
        query = query.union_all(totals_query)

    return query


def make_slicer_query_with_references(database, base_table, joins, dimensions, metrics, filters, reference_groups):
    """
    WRITEME

    :param database:
    :param base_table:
    :param joins:
    :param dimensions:
    :param metrics:
    :param filters:
    :param reference_groups:
    :return:
    """
    # Do not include totals dimensions in the reference query (they are selected directly in the container query)
    non_totals_dimensions = [dimension
                             for dimension in dimensions
                             if not isinstance(dimension, TotalsDimension)]

    original_query = make_slicer_query(database=database,
                                       base_table=base_table,
                                       joins=joins,
                                       dimensions=non_totals_dimensions,
                                       metrics=metrics,
                                       filters=filters) \
        .as_('base')

    container_query = database.query_cls.from_(original_query)

    ref_dimension_definitions, ref_terms = [], []
    for (ref_dimension, time_unit, interval), references in reference_groups.items():
        offset_func = partial(database.date_add,
                              date_part=time_unit,
                              interval=interval)

        # NOTE: In the case of weekly intervals with YoY references, the trunc date function needs to adjust for weekday
        # to keep things aligned. To do this, the date is first shifted forward a year before being truncated by week
        # and then shifted back.
        offset_for_weekday = weekly == ref_dimension.interval and YearOverYear.time_unit == time_unit
        ref_database = monkey_patch_align_weekdays(database, time_unit, interval) \
            if offset_for_weekday \
            else database

        ref_filters = make_reference_filters(filters,
                                             ref_dimension,
                                             offset_func)

        # Grab the first reference out of the list since all of the references in the group must share the same
        # reference type
        alias = references[0].reference_type.key
        ref_query = make_slicer_query(ref_database,
                                      base_table,
                                      joins,
                                      non_totals_dimensions,
                                      metrics,
                                      ref_filters) \
            .as_(alias)

        join_criterion = make_reference_join_criterion(ref_dimension,
                                                       non_totals_dimensions,
                                                       original_query,
                                                       ref_query,
                                                       offset_func)

        if join_criterion:
            container_query = container_query \
                .join(ref_query, JoinType.full_outer) \
                .on(join_criterion)
        else:
            container_query = container_query.from_(ref_query)

        ref_dimension_definitions.append([offset_func(ref_query.field(dimension.key))
                                          if ref_dimension == dimension
                                          else ref_query.field(dimension.key)
                                          for dimension in dimensions])

        ref_terms += make_terms_for_references(references,
                                               original_query,
                                               ref_query,
                                               metrics)

    dimension_terms = make_dimension_terms_for_reference_container_query(original_query,
                                                                         dimensions,
                                                                         ref_dimension_definitions)
    metric_terms = make_metric_terms_for_reference_container_query(original_query,
                                                                   metrics)

    all_terms = dimension_terms + metric_terms + ref_terms
    return container_query.select(*all_terms)


def make_terms_for_metrics(metrics):
    return [metric.definition.as_(metric.key)
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
    ).as_(dimension.key)

    # Include the display definition if there is one
    return [
        dimension_definition,
        dimension.display_definition.as_(dimension.display_key)
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

