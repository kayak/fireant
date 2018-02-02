from functools import partial
from typing import (
    Callable,
    Iterator,
)

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

from ..dimensions import (
    DatetimeDimension,
    Dimension,
)
from ..intervals import weekly
from ..metrics import Metric
from ..references import (
    Reference,
    YearOverYear,
)


def join_reference(reference: Reference,
                   metrics: Iterator[Metric],
                   dimensions: Iterator[Dimension],
                   ref_dimension: DatetimeDimension,
                   date_add: Callable,
                   original_query,
                   outer_query: QueryBuilder):
    ref_query = original_query.as_(reference.key)

    date_add = partial(date_add,
                       date_part=reference.time_unit,
                       interval=reference.interval,
                       # Only need to adjust this for YoY references with weekly intervals
                       align_weekday=weekly == ref_dimension.interval
                                     and YearOverYear.time_unit == reference.time_unit)

    # FIXME this is a bit hacky, need to replace the ref dimension term in all of the filters with the offset
    query_wheres = getattr(ref_query, '_wheres')
    if query_wheres:
        wheres = _apply_to_term_in_criterion(ref_dimension.definition,
                                             date_add(ref_dimension.definition),
                                             query_wheres)
        setattr(ref_query, '_wheres', wheres)

    # Join inner query
    join_criterion = _build_reference_join_criterion(ref_dimension,
                                                     dimensions,
                                                     original_query,
                                                     ref_query,
                                                     date_add)
    outer_query = outer_query \
        .join(ref_query, JoinType.left) \
        .on(join_criterion)

    # Add metrics
    ref_metric = _reference_metric(reference,
                                   original_query,
                                   ref_query)

    return outer_query.select(*[ref_metric(metric).as_("{}_{}".format(metric.key, reference.key))
                                for metric in metrics])


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


def _build_reference_join_criterion(dimension: Dimension,
                                    all_dimensions: Iterator[Dimension],
                                    original_query: QueryBuilder,
                                    ref_query: QueryBuilder,
                                    offset_func: Callable):
    """
    This builds the criterion for joining a reference query to the base query. It matches the referenced dimension
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
    join_criterion = original_query.field(dimension.key) == offset_func(field=ref_query.field(dimension.key))

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
