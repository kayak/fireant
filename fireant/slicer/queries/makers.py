import copy
from functools import partial

from typing import (
    Callable,
    Iterable,
)

from fireant.slicer.intervals import weekly
from fireant.slicer.references import (
    YearOverYear,
    reference_key,
    reference_term,
)
from fireant.utils import flatten
from pypika import JoinType
from pypika.queries import QueryBuilder
from pypika.terms import (
    ComplexCriterion,
    Criterion,
    NullValue,
    Term,
)
from .finders import (
    find_joins_for_tables,
    find_required_tables_to_join,
)
from ..dimensions import (
    Dimension,
    TotalsDimension,
)
from ..filters import DimensionFilter


def make_slicer_query(query_cls, trunc_date, base_table, joins=(), dimensions=(), metrics=(), filters=()):
    """
    Creates a pypika/SQL query from a list of slicer elements.

    This is the base implementation shared by two implementations: the query to fetch data for a slicer request and
    the query to fetch choices for dimensions.

    This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
    for the query to fetch choices for dimensions.

    The slicer query extends this with metrics, references, and totals.

    :param query_cls:
        pypika.Query - The pypika query class to use to create the query
    :param trunc_date:
        Callable - A function to truncate a date to an interval (database vendor specific)
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
    query = query_cls.from_(base_table)

    elements = flatten([metrics, dimensions, filters])

    # Add joins
    join_tables_needed_for_query = find_required_tables_to_join(elements, base_table)
    for join in find_joins_for_tables(joins, base_table, join_tables_needed_for_query):
        query = query.join(join.table, how=join.join_type).on(join.criterion)

    # Add dimensions
    for dimension in dimensions:
        terms = make_terms_for_dimension(dimension, trunc_date)
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


def make_slicer_query_with_totals(database, table, joins, dimensions, metrics, filters,
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
    query = make_slicer_query_with_references(database,
                                              table,
                                              joins,
                                              dimensions,
                                              metrics,
                                              filters,
                                              reference_groups)
    for dimension in totals_dimensions:
        totals_dimension_index = dimensions.index(dimension)
        grouped_dims, totaled_dims = dimensions[:totals_dimension_index], dimensions[totals_dimension_index:]

        dimensions_with_totals = grouped_dims + [TotalsDimension(dimension)
                                                 for dimension in totaled_dims]

        totals_query = make_slicer_query_with_references(database,
                                                         table,
                                                         joins,
                                                         dimensions_with_totals,
                                                         metrics,
                                                         filters,
                                                         reference_groups)

        # UNION ALL
        query = query.union_all(totals_query)

    return query


def make_slicer_query_with_references(database, base_table, joins, dimensions, metrics, filters, reference_groups):
    """
    WRITEME

    :param query_cls:
    :param trunc_date:
    :param base_table:
    :param joins:
    :param dimensions:
    :param metrics:
    :param filters:
    :param reference_groups:
    :return:
    """

    if not reference_groups:
        return make_slicer_query(query_cls=database.query_cls,
                                 trunc_date=database.trunc_date,
                                 base_table=base_table,
                                 joins=joins,
                                 dimensions=dimensions,
                                 metrics=metrics,
                                 filters=filters)

    # Do not include totals dimensions in the reference query (they are selected directly in the container query)
    non_totals_dimensions = [dimension
                             for dimension in dimensions
                             if not isinstance(dimension, TotalsDimension)]

    query = make_slicer_query(query_cls=database.query_cls,
                              trunc_date=database.trunc_date,
                              base_table=base_table,
                              joins=joins,
                              dimensions=non_totals_dimensions,
                              metrics=metrics,
                              filters=filters)

    original_query = query.as_('base')
    container_query = make_reference_container_query(original_query,
                                                     database.query_cls,
                                                     dimensions,
                                                     metrics)

    for (dimension, time_unit, interval), references in reference_groups.items():
        alias = references[0].key[:3]
        ref_query, join_criterion = make_reference_query(database,
                                                         base_table,
                                                         joins,
                                                         non_totals_dimensions,
                                                         metrics,
                                                         filters,
                                                         dimension,
                                                         time_unit,
                                                         interval,
                                                         original_query,
                                                         alias=alias)

        terms = make_terms_for_references(references,
                                          original_query,
                                          ref_query,
                                          metrics)

        container_query = container_query \
            .join(ref_query, JoinType.full_outer) \
            .on(join_criterion) \
            .select(*terms)

    return container_query


def make_reference_container_query(original_query, query_cls, dimensions, metrics):
    """
    Creates a container query with the original query used as the FROM clause and selects all of the metrics. The
    container query is used as a base for joining reference queries.

    :param original_query:
    :param query_cls:
    :param dimensions:
    :param metrics:
    :return:
    """

    def original_query_field(element, display=False):
        key = element.display_key if display else element.key
        definition = element.display_definition if display else element.definition

        if isinstance(definition, NullValue):
            # If an element is a literal NULL, then include the definition directly in the container query. It will be
            # omitted from the reference queries
            return definition.as_(key)

        return original_query.field(key).as_(key)

    outer_query = query_cls.from_(original_query)

    # Add dimensions
    for dimension in dimensions:
        outer_query = outer_query.select(original_query_field(dimension))

        if dimension.has_display_field:
            outer_query = outer_query.select(original_query_field(dimension, True))

    # Add base metrics
    return outer_query.select(*[original_query_field(metric)
                                for metric in metrics])


def make_reference_query(database, base_table, joins, dimensions, metrics, filters, ref_dimension, time_unit, interval,
                         original_query, alias=None):
    """
    WRITEME

    :param alias:
    :param database:
    :param base_table:
    :param joins:
    :param dimensions:
    :param metrics:
    :param filters:
    :param references:
    :param ref_dimension:
    :param time_unit:
    :param interval:
    :param original_query:
    :return:
    """
    offset_func = partial(database.date_add,
                          date_part=time_unit,
                          interval=interval)

    # for weekly interval dimensions with YoY references, correct the day of week by adding a year, truncating to a
    # week, then subtracting a year
    offset_for_weekday = weekly == ref_dimension.interval and YearOverYear.time_unit == time_unit
    if offset_for_weekday:
        def trunc_date(definition, _):
            shift_forward = database.date_add(definition, time_unit, interval)
            offset = database.trunc_date(shift_forward, weekly.key)
            return database.date_add(offset, time_unit, -interval)

    else:
        trunc_date = database.trunc_date

    ref_filters = make_reference_filters(filters,
                                         ref_dimension,
                                         offset_func)

    ref_query = make_slicer_query(database.query_cls,
                                  trunc_date,
                                  base_table,
                                  joins,
                                  dimensions,
                                  metrics,
                                  ref_filters) \
        .as_(alias)

    join_criterion = make_reference_join_criterion(ref_dimension,
                                                   dimensions,
                                                   original_query,
                                                   ref_query,
                                                   offset_func)

    return ref_query, join_criterion


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


def make_terms_for_references(references, original_query, ref_query, metrics):
    """
    Makes the terms needed to be selected from a reference query in the container query.

    :param references:
    :param original_query:
    :param ref_query:
    :param metrics:
    :return:
    """
    seen = set()
    terms = []
    for reference in references:
        # Don't select duplicate references twice
        if reference.key in seen \
              and not seen.add(reference.key):
            continue

        # Get function to select reference metrics
        ref_metric = reference_term(reference,
                                    original_query,
                                    ref_query)

        terms += [ref_metric(metric).as_(reference_key(metric, reference))
                  for metric in metrics]

    return terms


def make_reference_filters(filters, ref_dimension, offset_func):
    """
    Copies and replaces the reference dimension's definition in all of the filters applied to a slicer query.

    This is used to shift the dimension filters to fit the reference window.

    :param filters:
    :param ref_dimension:
    :param offset_func:
    :return:
    """
    offset_ref_dimension_definition = offset_func(ref_dimension.definition)

    reference_filters = []
    for ref_filter in map(copy.deepcopy, filters):
        ref_filter.definition = _apply_to_term_in_criterion(ref_dimension.definition,
                                                            offset_ref_dimension_definition,
                                                            ref_filter.definition)
        reference_filters.append(ref_filter)

    return reference_filters


def make_reference_join_criterion(ref_dimension: Dimension,
                                  all_dimensions: Iterable[Dimension],
                                  original_query: QueryBuilder,
                                  ref_query: QueryBuilder,
                                  offset_func: Callable):
    """
    This creates the criterion for joining a reference query to the base query. It matches the referenced dimension
    in the base query to the offset referenced dimension in the reference query and all other dimensions.

    :param ref_dimension:
        The referenced dimension.
    :param all_dimensions:
        All of the dimensions applied to the slicer query.
    :param original_query:
        The base query, the original query despite the references.
    :param ref_query:
        The reference query, a copy of the base query with the referenced dimension replaced.
    :param offset_func:
        The offset function for shifting the referenced dimension.
    :return:
        pypika.Criterion
    """
    join_criterion = original_query.field(ref_dimension.key) == offset_func(ref_query.field(ref_dimension.key))

    for dimension_ in all_dimensions:
        if ref_dimension == dimension_:
            continue

        join_criterion &= original_query.field(dimension_.key) == ref_query.field(dimension_.key)

    return join_criterion


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


def _apply_to_term_in_criterion(target: Term,
                                replacement: Term,
                                criterion: Criterion):
    """
    Finds and replaces a term within a criterion.  This is necessary for adapting filters used in reference queries
    where the reference dimension must be offset by some value.  The target term is found inside the criterion and
    replaced with the replacement.

    :param target:
        The target term to replace in the criterion. It will be replaced in all locations within the criterion with
        the func applied to itself.
    :param replacement:
        The replacement for the term.
    :param criterion:
        The criterion to replace the term in.
    :return:
        A criterion identical to the original criterion arg except with the target term replaced by the replacement arg.
    """
    if isinstance(criterion, ComplexCriterion):
        criterion.left = _apply_to_term_in_criterion(target, replacement, criterion.left)
        criterion.right = _apply_to_term_in_criterion(target, replacement, criterion.right)
        return criterion

    for attr in ['term', 'left', 'right']:
        if hasattr(criterion, attr) and str(getattr(criterion, attr)) == str(target):
            setattr(criterion, attr, replacement)

    return criterion
