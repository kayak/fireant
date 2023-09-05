import math
from datetime import (
    date,
    datetime,
)

import numpy as np
import pandas as pd

from fireant.dataset.fields import DataType
from fireant.dataset.totals import TOTALS_MARKERS
from fireant.utils import filter_kwargs

RAW_VALUE = "raw"
INF_VALUE = "Inf"
NAN_VALUE = "NaN"
NULL_VALUE = "null"
BLANK_VALUE = ""
TOTALS_VALUE = "$totals"
TOTALS_LABEL = "Totals"

epoch = np.datetime64(datetime.utcfromtimestamp(0))
milliseconds = np.timedelta64(1, "ms")

DATE_FORMATS = {
    "iso": "%Y-%m-%dT%H:%M:%S",
    "hour": "%Y-%m-%d %H:00",
    "day": "%Y-%m-%d",
    "week": "W%W %Y-%m-%d",
    "month": "%b %Y",
    "year": "%Y",
}


def quarter_from_month(month):
    return math.ceil(month / 3)


@filter_kwargs
def date_as_string(value, interval_key=None):
    if interval_key == 'quarter':
        # strftime does not support a quarter format placeholder
        return 'Q{quarter} {year}'.format(year=value.year, quarter=quarter_from_month(value.month))

    f = DATE_FORMATS.get(interval_key, "%Y-%m-%d")
    return value.strftime(f)


@filter_kwargs
def date_as_millis(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    return int(1000 * value.timestamp())


@filter_kwargs
def return_none(value):
    return None


@filter_kwargs
def _format_decimal(value, thousands="", precision=None, suffix=None, use_raw_value=False):
    if not isinstance(value, (int, float)):
        return value

    if use_raw_value and suffix == '%':
        # When raw values are required, we divide percentage values by 100 to ensure they
        # work well with Spreadsheet applications like Excel.
        value /= 100

        # Add extra precision to offset the division
        if precision is not None:
            precision += 2

    if use_raw_value:
        precision_pattern = f'{{:.{precision if precision is not None else 16}f}}'
    elif precision is not None:
        precision_pattern = f'{{:{thousands}.{precision}f}}'
    else:
        precision_pattern = f'{{:{thousands}f}}'

    value = precision_pattern.format(value)
    if precision is None:
        value = value.rstrip('0').rstrip('.')

    return value


@filter_kwargs
def wrap_styling(value, use_raw_value=False, prefix=None, suffix=None):
    if value is None or use_raw_value:
        return value

    return "{prefix}{value}{suffix}".format(prefix=prefix or "", suffix=suffix or "", value=value)


@filter_kwargs
def _identity(value):
    return value


def json_value(value):
    """
    This function will return only values safe for JSON
    """
    if pd.isnull(value):
        return None
    if isinstance(value, float) and np.isinf(value):
        return "inf"
    if value in TOTALS_MARKERS:
        return TOTALS_VALUE
    return value


def safe_value(value):
    if value is None or value == "" or pd.isnull(value):
        return NULL_VALUE
    if isinstance(value, float) and np.isinf(value):
        return "inf"
    if value in TOTALS_MARKERS:
        return TOTALS_VALUE

    return date_as_string(value, interval_key="iso") if isinstance(value, date) else str(value)


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
        The dataset field that the value represents.
    :param date_as:
    """
    if value is None or pd.isnull(value):
        return None
    if isinstance(value, float) and np.isinf(value):
        return None
    if value in TOTALS_MARKERS:
        return TOTALS_VALUE

    formatter = RAW_FIELD_FORMATTER.get(field.data_type, _identity)
    return formatter(value, date_as=date_as, interval_key="iso")


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


def display_value(
    value,
    field,
    date_as=date_as_string,
    nan_value=NAN_VALUE,
    null_value=NULL_VALUE,
    use_raw_value=False,
):
    """
    Converts a metric value into the display value by applying formatting.

    :param value:
        The raw metric value.
    :param field:
        The dataset field that the value represents.
    :param date_as:
        A format function for datetimes.
    :param nan_value:
        The value to return if the value is a Pandas null (np.nan) value.
    :param null_value:
        The value to return if the value is None
    :param use_raw_value:
        Do not output a value with prefix/suffixes. If a suffix is a percentage sign and this value is true, the raw
        value will be value/100 to make it easier for users when exporting to tools like Excel.

    :return:
        A formatted string containing the display value for the metric.
    """
    if value is None:
        return null_value
    if pd.isnull(value):
        return nan_value
    if isinstance(value, float) and np.isinf(value):
        return INF_VALUE
    if value in TOTALS_MARKERS:
        return TOTALS_LABEL

    format_kwargs = {
        key: getattr(field, key, None) for key in ("prefix", "suffix", "thousands", "precision", "interval_key")
    }
    format_kwargs = {key: value for key, value in format_kwargs.items() if value is not None}

    formatter = FIELD_DISPLAY_FORMATTER.get(field.data_type, _identity)
    return formatter(value, date_as=date_as, use_raw_value=use_raw_value, **format_kwargs)
