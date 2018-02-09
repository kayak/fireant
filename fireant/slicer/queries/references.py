from functools import partial

from pypika import (
    JoinType,

    functions as fn,
)
from pypika.queries import QueryBuilder
from pypika.terms import (
    ComplexCriterion,
    Criterion,
    Term,
)
from typing import (
    Callable,
    Iterable,
)

from ..dimensions import (
    DatetimeDimension,
    Dimension,
)
from ..intervals import weekly
from ..references import (
    Reference,
    YearOverYear,
    reference_key,
)


def create_container_query(original_query, query_cls, dimensions, metrics):
    """
    Creates a container query with the original query used as the FROM clause and selects all of the metrics. The
    container query is used as a base for joining reference queries.

    :param original_query:
    :param query_cls:
    :param dimensions:
    :param metrics:
    :return:
    """

    def original_query_field(key):
        return original_query.field(key).as_(key)

    outer_query = query_cls.from_(original_query)

    # Add dimensions
    for dimension in dimensions:
        outer_query = outer_query.select(original_query_field(dimension.key))

        if dimension.has_display_field:
            outer_query = outer_query.select(original_query_field(dimension.display_key))

    # Add base metrics
    return outer_query.select(*[original_query_field(metric.key)
                                for metric in metrics])


def create_joined_reference_query(time_unit: str,
                                  interval: int,
                                  dimensions: Iterable[Dimension],
                                  ref_dimension: DatetimeDimension,
                                  original_query,
                                  container_query,
                                  database):
    ref_query = original_query.as_('{}_ref'.format(ref_dimension.key))

    offset_func = partial(database.date_add,
                          date_part=time_unit,
                          interval=interval)

    _hack_fixes_into_the_query(database, interval, offset_func, ref_dimension, ref_query, time_unit)

    # Join inner query
    join_criterion = _create_reference_join_criterion(ref_dimension,
                                                      dimensions,
                                                      original_query,
                                                      ref_query,
                                                      offset_func)
    container_query = container_query \
        .join(ref_query, JoinType.left) \
        .on(join_criterion)

    return container_query, ref_query


def _hack_fixes_into_the_query(database, interval, offset_func, ref_dimension, ref_query, time_unit):
    # for weekly interval dimensions with YoY references, correct the day of week by adding a year, truncating to a
    # week, then subtracting a year
    offset_for_weekday = weekly == ref_dimension.interval and YearOverYear.time_unit == time_unit
    if offset_for_weekday:
        shift_forward = database.date_add(ref_dimension.definition, time_unit, interval)
        offset = database.trunc_date(shift_forward, 'week')
        shift_back = database.date_add(offset, time_unit, -interval)

        getattr(ref_query, '_selects')[0] = shift_back.as_(ref_dimension.key)

    # need to replace the ref dimension term in all of the filters with the offset
    query_wheres = getattr(ref_query, '_wheres')
    if query_wheres:
        wheres = _apply_to_term_in_criterion(ref_dimension.definition,
                                             offset_func(ref_dimension.definition),
                                             query_wheres)
        setattr(ref_query, '_wheres', wheres)


def select_reference_metrics(references, container_query, original_query, ref_query, metrics):
    """
    Select the metrics for a list of references with a reference query. The list of references is expected to share
    the same time unit and interval.

    :param references:
    :param container_query:
    :param original_query:
    :param ref_query:
    :param metrics:
    :return:
    """
    seen = set()
    for reference in references:
        # Don't select duplicate references twice
        if reference.key in seen:
            continue
        else:
            seen.add(reference.key)

        # Get function to select reference metrics
        ref_metric = _reference_metric(reference,
                                       original_query,
                                       ref_query)

        # Select metrics
        container_query = container_query.select(*[ref_metric(metric).as_(reference_key(metric, reference))
                                                   for metric in metrics])
    return container_query


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


def _create_reference_join_criterion(dimension: Dimension,
                                     all_dimensions: Iterable[Dimension],
                                     original_query: QueryBuilder,
                                     ref_query: QueryBuilder,
                                     offset_func: Callable):
    """
    This creates the criterion for joining a reference query to the base query. It matches the referenced dimension
    in the base query to the offset referenced dimension in the reference query and all other dimensions.

    :param dimension:
        The referenced dimension
    :param original_query:
        The base query, the original query despite the references.
    :param ref_query:
        The reference query, a copy of the base query with the referenced dimension replaced.
    :param offset_func:
        The offset function for shifting the referenced dimension.
    :return:
        pypika.Criterion
    """
    join_criterion = original_query.field(dimension.key) == offset_func(ref_query.field(dimension.key))

    for dimension_ in all_dimensions:
        if dimension == dimension_:
            continue

        join_criterion &= original_query.field(dimension_.key) == ref_query.field(dimension_.key)

    return join_criterion


def _reference_metric(reference: Reference,
                      original_query: QueryBuilder,
                      ref_query: QueryBuilder):
    """
    WRITEME

    :param reference:
    :param original_query:
    :param ref_query:
    :return:
    """

    def ref_field(metric):
        return ref_query.field(metric.key)

    if reference.is_delta:
        if reference.is_percent:
            return lambda metric: (original_query.field(metric.key) - ref_field(metric)) \
                                  * \
                                  (100 / fn.NullIf(ref_field(metric), 0))

        return lambda metric: original_query.field(metric.key) - ref_field(metric)

    return ref_field
