import sys

import numpy as np
import pandas as pd

MAX_TIMESTAMP = pd.Timestamp.max
MAX_NUMBER = sys.maxsize
MAX_STRING = '~~totals'

TOTALS_MARKERS = {MAX_STRING, MAX_NUMBER, MAX_TIMESTAMP}


def get_totals_marker_for_dtype(dtype):
    return {
        np.dtype('<M8[ns]'): MAX_TIMESTAMP,
        np.dtype('int64'): MAX_NUMBER,
    }.get(dtype, MAX_STRING)


def scrub_totals_from_share_results(data_frame, dimensions):
    if not isinstance(data_frame.index, pd.MultiIndex):
        if not dimensions or dimensions[0].is_rollup:
            return data_frame

        marker = get_totals_marker_for_dtype(data_frame.index.dtype)
        is_totals = data_frame.index == marker

        return data_frame[~is_totals]

    rollup_dimensions = np.array([dimension.is_rollup
                                  for dimension in dimensions])

    markers = np.array([get_totals_marker_for_dtype(level.dtype)
                        for level in data_frame.index.levels])
    is_totals = pd.DataFrame([values == markers
                              for values in data_frame.index],
                             index=data_frame.index)

    not_totals = (~is_totals).all(axis=1)
    is_rollup = ~is_totals.dot(~rollup_dimensions)

    # pad rollup dimensions to the right
    keep = (rollup_dimensions & is_totals).any(axis=1)
    # remove = ~(~rollup_dimensions & is_totals)
    # mask = (keep | remove).all(axis=1)
    # mask = not_totals | is_rollup
    mask = keep | ~is_totals.dot(~rollup_dimensions)
    return data_frame.loc[mask]
