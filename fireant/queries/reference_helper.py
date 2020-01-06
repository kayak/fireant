import copy
from functools import partial

from fireant.dataset.fields import Field
from .field_helper import make_term_for_dimension
from .finders import find_field_in_modified_field


def adapt_for_reference_query(
    reference_parts, database, dimensions, metrics, filters, references
):
    if reference_parts is None:
        return dimensions, metrics, filters

    ref_dimension, time_unit, interval = reference_parts
    # Unpack rolled up dimensions
    ref_dimension = find_field_in_modified_field(ref_dimension)

    ref_metrics = _make_reference_metrics(metrics, references[0].reference_type.alias)
    offset_func = partial(database.date_add, date_part=time_unit, interval=interval)
    ref_dimensions = _make_reference_dimensions(
        dimensions, ref_dimension, offset_func, database.trunc_date
    )
    ref_filters = _make_reference_filters(filters, ref_dimension, offset_func)
    return ref_dimensions, ref_metrics, ref_filters


def _replace_reference_dimension(dimension, offset_func, trunc_date=None):
    ref_definition = offset_func(make_term_for_dimension(dimension, trunc_date))
    field = Field(
        alias=dimension.alias,
        definition=ref_definition,
        data_type=dimension.data_type,
        label=dimension.label,
    )

    if hasattr(dimension, "for_"):
        return dimension.for_(field)

    return field


def _make_reference_dimensions(dimensions, ref_dimension, offset_func, trunc_date):
    return [
        _replace_reference_dimension(dimension, offset_func, trunc_date)
        if ref_dimension is find_field_in_modified_field(dimension)
        else dimension
        for dimension in dimensions
    ]


def _make_reference_metrics(metrics, ref_key):
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
            data_type=metric.data_type,
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
        if ref_filter.field is ref_dimension:
            offset_ref_field = _replace_reference_dimension(ref_dimension, offset_func)
            ref_filter = ref_filter.for_(offset_ref_field)

        reference_filters.append(ref_filter)

    return reference_filters
