import numpy as np
import pandas as pd

from fireant import utils
from fireant.formats import (
    INF_VALUE,
    NAN_VALUE,
)
from ..references import reference_label


def extract_display_values(dimensions, data_frame):
    """
    Retrieves the display values for each dimension.

    For UniqueDimension, this will retrieve the display values from the data frame containing the data from the slicer
    query. For CategoricalDimension, the values are retrieved from the set of display values configured in the slicer.

    :param dimensions:
        A list of dimensions present in a slicer query.
    :param data_frame:
        The data frame containing the data result of the slicer query.
    :return:
        A dict containing keys for dimensions with display values (If there are no display values then the
        dimension's key will not be present). The value of the dict will be either a dict or a data frame where the
        display value can be accessed using the display value as the key.
    """
    display_values = {}

    for dimension in dimensions:
        key = utils.format_dimension_key(dimension.key)

        if hasattr(dimension, 'display_values'):
            display_values[key] = dimension.display_values

        elif dimension.has_display_field:
            f_display_key = utils.format_dimension_key(dimension.display_key)

            display_values[key] = data_frame[f_display_key] \
                .groupby(level=key) \
                .first() \
                .fillna(value=NAN_VALUE) \
                .replace([np.inf, -np.inf], INF_VALUE)

    return display_values


def dimensional_metric_label(dimensions, dimension_display_values):
    """
    Creates a callback function for rendering series labels.

    :param dimensions:
        A list of fireant.Dimension which is being rendered.

    :param dimension_display_values:
        A dictionary containing key-value pairs for each dimension.
    :return:
        a callback function which renders a label for a metric, reference, and list of dimension values.
    """

    def render_series_label(dimension_values, metric=None, reference=None):
        """
        Returns a string label for a metric, reference, and set of values for zero or more dimensions.

        :param metric:
            an instance of fireant.Metric
        :param reference:
            an instance of fireant.Reference
        :param dimension_values:
            a tuple of dimension values. Can be zero-length or longer.
        :return:
        """
        num_used_dimensions = len(dimensions) - len(dimension_values)
        used_dimensions = dimensions[num_used_dimensions:]

        dimension_values = utils.wrap_list(dimension_values)
        dimension_labels = [utils.getdeepattr(dimension_display_values,
                                              (utils.format_dimension_key(dimension.key), dimension_value),
                                              dimension_value)
                            if not pd.isnull(dimension_value)
                            else 'Totals'
                            for dimension, dimension_value in zip(used_dimensions, dimension_values)]

        label = ", ".join([str(label) for label in dimension_labels])

        if metric is None:
            if reference is not None:
                return '{} ({})'.format(label, reference.label)
            return label

        if dimension_labels:
            return '{} ({})'.format(reference_label(metric, reference),
                                    label)

        return reference_label(metric, reference)

    return render_series_label
