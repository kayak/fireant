import sys

import numpy as np
import pandas as pd

from .modifiers import Rollup

DATE_TOTALS = pd.Timestamp.max
NUMBER_TOTALS = sys.maxsize
TEXT_TOTALS = '~~totals'

TOTALS_MARKERS = {TEXT_TOTALS, NUMBER_TOTALS, DATE_TOTALS}


def get_totals_marker_for_dtype(dtype):
    """
    For a given dtype, return the index value to use to indicate that the data frame row contains totals.

    :param dtype:
    :return:
    """
    return {
        np.dtype('<M8[ns]'): DATE_TOTALS,
        np.dtype('int64'): NUMBER_TOTALS,
        np.dtype('float64'): NUMBER_TOTALS,
    }.get(dtype, TEXT_TOTALS)


def scrub_totals_from_share_results(data_frame, dimensions):
    """
    This function returns a data frame with the values for dimension totals filtered out if the corresponding dimension
    was not queried with rollup. This comes into play when the share operation is used on metrics which requires the
    totals across the values.

    There are two different logical branches for this function that perform the same work, one on a single-level index
    data frame and another for a multi-level index.

    :param data_frame:
        The result data set.
    :param dimensions:
        A list of dimensions that were queried for to produce the result data set.
    :return:
        The data frame with totals rows removed for dimensions that were not queried with rollup.
    """
    if isinstance(data_frame.index, pd.MultiIndex):
        return _scrub_totals_for_multilevel_index_df(data_frame, dimensions)
    return _scrub_totals_for_singlelevel_index_df(data_frame, dimensions)


def _scrub_totals_for_singlelevel_index_df(data_frame, dimensions):
    # For this function, there will only ever be zero or 1 dimensions in the list of dimensions.
    # If there are zero dimensions or the only dimension is rolled up, then return the data frame as-is
    is_rolled_up = dimensions and isinstance(dimensions[0], Rollup)
    if is_rolled_up:
        return data_frame

    # Otherwise, remove any rows where the index value equals the totals marker for its dtype.
    marker = get_totals_marker_for_dtype(data_frame.index.dtype)
    is_totals = data_frame.index == marker
    return data_frame[~is_totals]


def _scrub_totals_for_multilevel_index_df(data_frame, dimensions):
    if data_frame.empty:
        return data_frame

    # Get the totals marker value for each index level
    markers = [get_totals_marker_for_dtype(level.dtype)
               for level in data_frame.index.levels]

    # Create a boolean data frame indicating whether or not the index value equals the totals marker for the dtype
    # corresponding to each index level
    is_total_marker = pd.DataFrame([[value == marker
                                     for value, marker in zip(values, markers)]
                                    for values in data_frame.index],
                                   index=data_frame.index)

    """
    If a row in the data frame is for totals for one index level, all of the subsequent index levels will also use a
    totals marker. In order to avoid filtering the wrong rows, a new data frame is created similar to `is_totals_marker`
    except a cell is only set to True if that value is a totals marker for the corresponding index level, the leaves of
    the dimension value tree.

    This is achieved by rolling an XOR function across each index level with the previous level.
    """
    first_column = is_total_marker.columns[0]
    is_totals_marker_leaf = pd.DataFrame(is_total_marker[first_column])
    for column, prev_column in zip(is_total_marker.columns[1:], list(is_total_marker.columns[:-1])):
        is_totals_marker_leaf[column] = np.logical_xor(is_total_marker[column], is_total_marker[prev_column])

    # Create a boolean vector for each dimension to mark if that dimension is rolled up
    rollup_dimensions = np.array([isinstance(dimension, Rollup)
                                  for dimension in dimensions])

    # Create a boolean pd.Series where False means to remove the row from the data frame.
    mask = (~(~rollup_dimensions & is_totals_marker_leaf)).all(axis=1)
    return data_frame.loc[mask]
