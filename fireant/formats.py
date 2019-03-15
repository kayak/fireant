import re

import numpy as np
import pandas as pd
from datetime import (
    date,
    datetime,
)

from fireant.dataset.fields import DataType
from fireant.dataset.totals import TOTALS_MARKERS
from fireant.utils import (
    filter_kwargs,
)

RAW_VALUE = 'raw'
INF_VALUE = "Inf"
NULL_VALUE = 'null'
BLANK_VALUE = ''
TOTALS_VALUE = '$totals'
TOTALS_LABEL = 'Totals'

epoch = np.datetime64(datetime.utcfromtimestamp(0))
milliseconds = np.timedelta64(1, 'ms')

DATE_FORMATS = {
    'iso': '%Y-%m-%dT%H:%M:%S',
    'hour': '%Y-%m-%d %H:00',
    'day': '%Y-%m-%d',
    'week': 'W%W %Y-%m-%d',
    'month': '%b %Y',
    'quarter': '%Y-%q',
    'year': '%Y',
}


@filter_kwargs
def date_as_string(value, interval_key=None):
    f = DATE_FORMATS.get(interval_key, '%Y-%m-%d')
    return value.strftime(f)


@filter_kwargs
def date_as_millis(value):
    if not isinstance(value, date):
        value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    return int(1000 * value.timestamp())


@filter_kwargs
def return_none(value):
    return None


@filter_kwargs
def _format_decimal(value, thousands=',', precision=None):
    precision_pattern = '{{:{}.{}f}}'.format(thousands, precision) \
        if precision is not None \
        else '{{:{}f}}'.format(thousands)

    f_value = precision_pattern.format(value)
    if precision is None:
        f_value = f_value.rstrip('0').rstrip('.')
    return f_value


@filter_kwargs
def wrap_styling(value, prefix=None, suffix=None):
    if value is None:
        return value

    return '{prefix}{value}{suffix}' \
        .format(prefix=prefix or '',
                suffix=suffix or '',
                value=value)


@filter_kwargs
def _identity(value):
    return value


UNSAFE_CHARS = re.compile(r'[^\w\d\-:()]')


def safe_value(value):
    if value is None or value is '' or pd.isnull(value):
        return NULL_VALUE
    if isinstance(value, float) and np.isinf(value):
        return 'inf'
    if value in TOTALS_MARKERS:
        return TOTALS_VALUE

    str_value = date_as_string(value, interval_key='iso') \
        if isinstance(value, date) \
        else str(value)
    return UNSAFE_CHARS.sub('$', str_value)


@filter_kwargs
def _format_date_field(value, date_as=date_as_string, **kwargs):
    return date_as(value, **kwargs)


RAW_FIELD_FORMATTER = {
    DataType.date: _format_date_field,
}


def raw_value(value, field, date_as=date_as_string):
    """
    Converts a raw metric value into a safe type. This will change dates into strings, NaNs into Nones, and np types
    into their corresponding python types.

    :param value:
        The raw metric value.
    :param field:
        The slicer field that the value represents.
    :param date_as:
    """
    if value is None or pd.isnull(value):
        return None
    if isinstance(value, float) and np.isinf(value):
        return INF_VALUE
    if value in TOTALS_MARKERS:
        return TOTALS_VALUE

    formatter = RAW_FIELD_FORMATTER.get(field.data_type, _identity)
    return formatter(value,
                     date_as=date_as,
                     interval_key='iso')


def _format_number_field_value(value, **kwargs):
    number = _format_decimal(value, **kwargs)
    return wrap_styling(number, **kwargs)


@filter_kwargs
def _format_boolean_field(value):
    return str(value).lower()


FIELD_DISPLAY_FORMATTER = {
    DataType.date: _format_date_field,
    DataType.number: _format_number_field_value,
    DataType.boolean: _format_boolean_field,
    DataType.text: return_none,
}


def display_value(value, field, date_as=date_as_string):
    """
    Converts a metric value into the display value by applying formatting.

    :param value:
        The raw metric value.
    :param field:
        The slicer field that the value represents.
    :param date_as:
        A format function for datetimes.
    :return:
        A formatted string containing the display value for the metric.
    """
    if pd.isnull(value):
        return BLANK_VALUE
    if isinstance(value, float) and np.isinf(value):
        return INF_VALUE
    if value in TOTALS_MARKERS:
        return TOTALS_LABEL

    format_kwargs = {key: getattr(field, key, None)
                     for key in ('prefix', 'suffix', 'thouands', 'precision', 'interval_key')}

    formatter = FIELD_DISPLAY_FORMATTER.get(field.data_type, _identity)
    return formatter(value,
                     date_as=date_as,
                     **format_kwargs)
