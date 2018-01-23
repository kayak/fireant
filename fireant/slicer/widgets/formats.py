import locale

import numpy as np
import pandas as pd
from datetime import (
    date,
    time,
)

NO_TIME = time(0)


def safe(value):
    if isinstance(value, date):
        if not hasattr(value, 'time') or value.time() == NO_TIME:
            return value.strftime('%Y-%m-%d')
        else:
            return value.strftime('%Y-%m-%dT%H:%M:%S')

    if value is None or (isinstance(value, float) and np.isnan(value)) or pd.isnull(value):
        return None

    if isinstance(value, np.int64):
        # Cannot transform np.int64 to json
        return int(value)

    if isinstance(value, np.float64):
        return float(value)

    return value


def display(value, prefix=None, suffix=None, precision=None):
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

    return '{prefix}{value}{suffix}'.format(
          prefix=prefix or '',
          value=str(value),
          suffix=suffix or '',
    )
