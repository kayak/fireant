import pandas as pd

from pypika.terms import Field
from fireant.dataset.references import Reference
from fireant.dataset.filters import ComparisonOperator
from fireant.utils import alias_selector


def reference_alias(metric: Field, reference: Reference) -> str:
    """
    Format a metric key for a reference.

    :return:
        A string that is used as the key for a reference metric.
    """
    key = metric.alias

    if reference is None:
        return key

    return '{}_{}'.format(key, reference.alias)


def reference_type_alias(metric: Field, reference: Reference) -> str:
    """
    Format a metric key for a subquery selection in case of a reference.

    :return:
        A string that is used as the selector for a field in a blended subquery.
    """
    key = metric.alias

    if reference is None:
        return key

    return '{}_{}'.format(key, reference.reference_type.alias)


def reference_label(metric: Field, reference: Reference) -> str:
    """
    Format a metric label for a reference.

    :return:
        A string that is used as the display value for a reference metric.
    """
    label = str.strip(metric.label or metric.alias)

    if reference is None:
        return label

    return '{} {}'.format(label, reference.label)


def reference_prefix(metric: Field, reference: Reference) -> str:
    """
    Return the prefix for a metric displayed for a reference (or no Reference)

    :return:
        A string that is used as the prefix for a reference metric.
    """
    if reference is not None and reference.delta_percent:
        return None
    return metric.prefix


def reference_suffix(metric: Field, reference: Reference) -> str:
    """
    Return the suffix for a metric displayed for a reference (or no Reference)

    :return:
        A string that is used as the suffix for a reference metric.
    """
    if reference is not None and reference.delta_percent:
        return '%'
    return metric.suffix


def apply_reference_filters(df: pd.DataFrame, reference: Reference) -> pd.DataFrame:
    for reference_filter in reference.filters:
        df_column_key = alias_selector(reference_alias(reference_filter.metric, reference))
        if df_column_key in df:
            column = df[df_column_key]
            dataframe_filter = ComparisonOperator.eval(column, reference_filter.operator, reference_filter.value)
            df = df.loc[dataframe_filter]

    return df
