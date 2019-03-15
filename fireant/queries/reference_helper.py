import copy
from functools import partial

from fireant.dataset.fields import Field
from fireant.dataset.intervals import (
    DatetimeInterval,
    week,
)
from fireant.dataset.references import YearOverYear
from pypika.terms import (
    ComplexCriterion,
    Criterion,
    Term,
)


def adapt_for_reference_query(reference_parts, database, dimensions, metrics, filters, references):
    if reference_parts is None:
        return database, dimensions, metrics, filters

    ref_dimension, time_unit, interval = reference_parts
    # Each group is guaranteed to have one reference and they will all be grouped together by reference type
    ref_database = _make_reference_database(database,
                                            ref_dimension,
                                            time_unit,
                                            interval)
    ref_metrics = _make_reference_metrics(metrics,
                                          references[0].reference_type.alias)
    offset_func = partial(database.date_add,
                          date_part=time_unit,
                          interval=interval)
    ref_dimensions = _make_reference_dimensions(dimensions,
                                                ref_dimension,
                                                offset_func)
    ref_filters = _make_reference_filters(filters,
                                          ref_dimension,
                                          offset_func)
    return ref_database, ref_dimensions, ref_metrics, ref_filters


def _make_reference_database(database, ref_dimension, time_unit, interval):
    # NOTE: In the case of weekly intervals with YoY references, the trunc date function needs to adjust for weekday
    # to keep things aligned. To do this, the date is first shifted forward a year before being truncated by week
    # and then shifted back.
    offset_for_weekday = isinstance(ref_dimension, DatetimeInterval) \
                         and ref_dimension.interval_key == week.keywords['interval_key'] \
                         and YearOverYear.time_unit == time_unit

    if offset_for_weekday:
        return _monkey_patch_align_weekdays(database, time_unit, interval)

    return database


def _make_reference_dimensions(dimensions, ref_dimension, offset_func):
    def replace_reference_dimension(dimension):
        ref_dimension = copy.copy(dimension)
        if hasattr(ref_dimension, 'dimension'):
            ref_dimension.dimension = copy.copy(dimension.dimension)
        ref_dimension.definition = offset_func(ref_dimension.definition)
        return ref_dimension

    return [replace_reference_dimension(dimension)
            if dimension is ref_dimension
            else dimension
            for dimension in dimensions]


def _make_reference_metrics(metrics, ref_key):
    return [Field(metric.alias + '_' + ref_key,
                  metric.definition,
                  label=metric.label,
                  prefix=metric.prefix,
                  suffix=metric.suffix,
                  precision=metric.precision)
            for metric in metrics]


def _make_reference_filters(filters, ref_dimension, offset_func):
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


def _monkey_patch_align_weekdays(database, time_unit, interval):
    original_trunc_date = database.__class__.trunc_date

    def trunc_date(definition, _):
        offset = original_trunc_date(database, definition, week.keywords['interval_key'])
        return database.date_add(offset, time_unit, -interval)

    # Copy the database to avoid side effects then monkey patch the trunc date function with the correction for weekday
    database = copy.deepcopy(database)
    database.trunc_date = trunc_date
    return database


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
