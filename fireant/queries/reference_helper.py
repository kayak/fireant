import copy
from functools import partial

from fireant.dataset.fields import Field
from fireant.dataset.modifiers import Rollup
from .field_helper import make_term_for_dimension


def adapt_for_reference_query(
    dataset, reference_parts, database, dimensions, metrics, filters, references
):
    if reference_parts is None:
        return dimensions, metrics, filters

    ref_dimension, time_unit, interval = reference_parts
    # Unpack rolled up dimensions
    ref_dimension = (
        ref_dimension.dimension if isinstance(ref_dimension, Rollup) else ref_dimension
    )

    ref_metrics = _make_reference_metrics(
        dataset, metrics, references[0].reference_type.alias
    )
    offset_func = partial(database.date_add, date_part=time_unit, interval=interval)
    ref_dimensions = _make_reference_dimensions(
        database, dimensions, ref_dimension, offset_func
    )
    ref_filters = _make_reference_filters(filters, ref_dimension, offset_func)
    return ref_dimensions, ref_metrics, ref_filters


def _make_reference_dimensions(database, dimensions, ref_dimension, offset_func):
    def replace_reference_dimension(dimension):
        ref_dimension_copy = copy.copy(dimension)
        if hasattr(ref_dimension_copy, "dimension"):
            ref_dimension_copy.dimension = copy.copy(dimension.dimension)

        ref_definition = make_term_for_dimension(
            ref_dimension_copy, database.trunc_date
        )
        ref_dimension_copy.definition = offset_func(ref_definition)
        return ref_dimension_copy

    return [
        replace_reference_dimension(dimension)
        if dimension is ref_dimension
        else dimension
        for dimension in dimensions
    ]


def _make_reference_metrics(dataset, metrics, ref_key):
    metric_copies = []

    for metric in [copy.deepcopy(metric) for metric in metrics]:
        for pypika_field in metric.definition.fields_():
            if pypika_field.name.startswith("$"):
                pypika_field.name = "{}_{}".format(pypika_field.name, ref_key)

        metric_copies.append(metric)

    return [
        Field(
            "{}_{}".format(metric.alias, ref_key),
            metric.definition,
            label=metric.label,
            prefix=metric.prefix,
            suffix=metric.suffix,
            precision=metric.precision,
        )
        for metric in metric_copies
    ]


def _make_reference_filters(filters, ref_dimension, offset_func):
    """
    Copies and replaces the reference dimension's definition in all of the filters applied to a dataset query.

    This is used to shift the dimension filters to fit the reference window.

    :param filters:
    :param ref_dimension:
    :param offset_func:
    :return:
    """
    offset_ref_dimension_definition = offset_func(ref_dimension.definition)

    reference_filters = []
    for ref_filter in filters:
        if ref_filter.field.alias == ref_dimension.alias:
            ref_filter = copy.deepcopy(ref_filter)
            ref_filter.field.definition = offset_ref_dimension_definition
        reference_filters.append(ref_filter)

    return reference_filters
