def reference_alias(metric, reference):
    """
    Format a metric key for a reference.

    :return:
        A string that is used as the key for a reference metric.
    """
    key = metric.alias

    if reference is None:
        return key

    return '{}_{}'.format(key, reference.alias)


def reference_label(metric, reference):
    """
    Format a metric label for a reference.

    :return:
        A string that is used as the display value for a reference metric.
    """
    label = metric.label or metric.alias

    if reference is None:
        return label

    return '{} {}'.format(label, reference.label)


def reference_prefix(metric, reference):
    """
    Return the prefix for a metric displayed for a reference (or no Reference)

    :return:
        A string that is used as the prefix for a reference metric.
    """
    if reference is not None and reference.delta_percent:
        return None
    return metric.prefix


def reference_suffix(metric, reference):
    """
    Return the suffix for a metric displayed for a reference (or no Reference)

    :return:
        A string that is used as the suffix for a reference metric.
    """
    if reference is not None and reference.delta_percent:
        return '%'
    return metric.suffix
