import copy

from fireant.dataset.fields import Field, is_metric_field
from fireant.queries.finders import find_field_in_modified_field


def _replace_reference_dimension(dimension, offset_func, field_transformer, trunc_date=None):
    ref_definition = offset_func(field_transformer(dimension, trunc_date))
    field = Field(
        alias=dimension.alias,
        definition=ref_definition,
        data_type=dimension.data_type,
        label=dimension.label,
    )

    if hasattr(dimension, "for_"):
        return dimension.for_(field)

    return field


def make_reference_dimensions(dimensions, ref_dimension, offset_func, field_transformer, trunc_date):
    return [
        _replace_reference_dimension(dimension, offset_func, field_transformer, trunc_date)
        if ref_dimension is find_field_in_modified_field(dimension)
        else dimension
        for dimension in dimensions
    ]


def make_reference_metrics(metrics, ref_key):
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
        for metric in metrics
    ]


def make_reference_filters(filters, ref_dimension, offset_func):
    """
    Copies and replaces the reference dimension's definition in all of the filters applied to a dataset query.

    This is used to shift the dimension filters to fit the reference window.

    :param filters:
    :param ref_dimension:
    :param offset_func:
    :return:
    """
    reference_filters = []
    for ref_filter in filters:
        # Metric filters should not be part of the reference
        if is_metric_field(ref_filter.field):
            continue

        if ref_filter.field is ref_dimension:
            # NOTE: Important to apply the offset function to the start and stop properties because the date math can
            # become expensive over many rows
            ref_filter = copy.copy(ref_filter)
            ref_filter.start = offset_func(ref_filter.start)
            ref_filter.stop = offset_func(ref_filter.stop)

        reference_filters.append(ref_filter)

    return reference_filters
