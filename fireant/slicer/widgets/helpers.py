from fireant import utils


def extract_display_values(dimensions, data_frame):
    """

    :param dimensions:
    :param data_frame:
    :return:
    """
    dv_by_dimension = {}

    for dimension in dimensions:
        dkey = dimension.key

        if hasattr(dimension, 'display_values'):
            dv_by_dimension[dkey] = dimension.display_values

        elif hasattr(dimension, 'display_key'):
            dv_by_dimension[dkey] = data_frame[dimension.display_key].groupby(level=dkey).first()

    return dv_by_dimension


def reference_key(metric, reference):
    key = metric.key

    if reference is not None:
        return '{}_{}'.format(key, reference.key)

    return key


def reference_label(metric, reference):
    label = metric.label or metric.key

    if reference is not None:
        return '{} ({})'.format(label, reference.label)

    return label


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

    def render_series_label(metric, reference, dimension_values):
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
        dimension_labels = [utils.deep_get(dimension_display_values,
                                           [dimension.key, dimension_value],
                                           dimension_value)
                            for dimension, dimension_value in zip(dimensions[1:], dimension_values)]

        if dimension_labels:
            return '{} ({})'.format(reference_label(metric, reference),
                                    ', '.join(dimension_labels))

        return metric.label

    return render_series_label
