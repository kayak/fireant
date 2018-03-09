import locale
from datetime import (
    date,
    datetime,
    time,
)

import numpy as np
import pandas as pd

INFINITY = "Infinity"

NO_TIME = time(0)

epoch = np.datetime64(datetime.utcfromtimestamp(0))
milliseconds = np.timedelta64(1, 'ms')


def date_as_millis(value):
    if not isinstance(value, date):
        value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    return int(1000 * value.timestamp())


def coerce_type(value):
    for type_cast in (int, float):
        try:
            return type_cast(value)
        except:
            pass

    if 'null' == value:
        return None
    if 'True' == value:
        return True
    if 'False' == value:
        return False

    return value


def dimension_value(value):
    """
    Format a dimension value. This will coerce the raw string or date values into a proper primitive value like a
    string, float, or int.

    :param value:
        The raw str or datetime value
    :param str_date:
        When True, dates and datetimes will be converted to ISO strings. The time is omitted for dates. When False, the
        datetime will be converted to a POSIX timestamp (millis-since-epoch).
    """
    if pd.isnull(value):
        return 'Totals'

    if isinstance(value, date):
        if not hasattr(value, 'time') or value.time() == NO_TIME:
            return value.strftime('%Y-%m-%d')
        else:
            return value.strftime('%Y-%m-%d %H:%M:%S')

    return coerce_type(value)


def metric_value(value):
    """
    Converts a raw metric value into a safe type. This will change dates into strings, NaNs into Nones, and np types
    into their corresponding python types.

    :param value:
        The raw metric value.
    """
    if isinstance(value, date):
        if not hasattr(value, 'time') or value.time() == NO_TIME:
            return value.strftime('%Y-%m-%d')
        else:
            return value.strftime('%Y-%m-%dT%H:%M:%S')

    if value is None or pd.isnull(value):
        return None

    if isinstance(value, float):
        if np.isinf(value):
            return None
        return float(value)

    if isinstance(value, np.int64):
        # Cannot transform np.int64 to json
        return int(value)

    return value


def metric_display(value, prefix=None, suffix=None, precision=None):
    """
    Converts a metric value into the display value by applying formatting.

    :param value:
        The raw metric value.
    :param prefix:
        An optional prefix.
    :param suffix:
        An optional suffix.
    :param precision:
        The decimal precision, the number of decimal places to round to.
    :return:
        A formatted string containing the display value for the metric.
    """
    if isinstance(value, float):
        if precision is not None:
            float_format = '%d' if precision == 0 else '%.{}f'.format(precision)
            value = locale.format(float_format, value, grouping=True)

        elif value.is_integer():
            float_format = '%d'
            value = locale.format(float_format, value, grouping=True)

        else:
            float_format = '%f'
            # Stripping trailing zeros is necessary because %f can add them if no precision is set
            value = locale.format(float_format, value, grouping=True).rstrip('.0')

    if isinstance(value, int):
        float_format = '%d'
        value = locale.format(float_format, value, grouping=True)

    if value is INFINITY:
        return "∞"

    return '{prefix}{value}{suffix}'.format(
          prefix=prefix or '',
          value=str(value),
          suffix=suffix or '',
    )
