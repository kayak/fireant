import time
from typing import Iterable

import pandas as pd

from fireant.database.base import Database
from .logger import logger
from ..dimensions import (
    ContinuousDimension,
    Dimension,
)

NULL_VALUE = 'null'


def fetch_data(database: Database, query: str, dimensions: Iterable[Dimension]):
    """
    Executes a query to fetch data from database middleware and builds/cleans the data as a data frame. The query
    execution is logged with its duration.

    :param database:
        instance of `fireant.Database`, database middleware
    :param query: Query string
    :param dimensions: A list of dimensions, used for setting the index on the result data frame.

    :return: `pd.DataFrame` constructed from the result of the query
    """
    start_time = time.time()
    logger.debug(query)

    data_frame = database.fetch_data(str(query))

    logger.info('[{duration} seconds]: {query}'
                .format(duration=round(time.time() - start_time, 4),
                        query=query))

    return clean_and_apply_index(data_frame, dimensions)


def clean_and_apply_index(data_frame: pd.DataFrame, dimensions: Iterable[Dimension]):
    """
    Sets the index on a data frame. This will also replace any nulls from the database with an empty string for
    non-continuous dimensions. Totals will be indexed with Nones.

    :param data_frame:
    :param dimensions:
    :return:
    """
    if not dimensions:
        return data_frame

    dimension_keys = [d.key for d in dimensions]

    for i, dimension in enumerate(dimensions):
        if isinstance(dimension, ContinuousDimension):
            # Continuous dimensions are can never contain null values since they are selected as windows of values
            # With that in mind, we leave the NaNs in them to represent Totals.
            continue

        level = dimension.key
        data_frame[level] = fill_nans_in_level(data_frame, dimension, dimension_keys[:i]) \
            .apply(lambda x: str(x) if not pd.isnull(x) else None)

    # Set index on dimension columns
    return data_frame.set_index(dimension_keys)


def fill_nans_in_level(data_frame, dimension, preceding_dimension_keys):
    """
    In case there are NaN values representing both totals (from ROLLUP) and database nulls, we need to replace the real
    nulls with an empty string in order to distinguish between them.  We choose to replace the actual database nulls
    with an empty string in order to let NaNs represent Totals because mixing string values in the pandas index types
    used by continuous dimensions does work.

    :param data_frame:
        The data_frame we are replacing values in.
    :param dimension:
        The level of the data frame to replace nulls in. This function should be called once per non-conitnuous
        dimension, in the order of the dimensions.
    :param preceding_dimension_keys:
    :return:
        The level in the data_frame with the nulls replaced with empty string
    """
    level = dimension.key

    if dimension.is_rollup:
        if preceding_dimension_keys:
            return (data_frame
                    .groupby(preceding_dimension_keys)[level]
                    .apply(_fill_nan_for_nulls))

        return _fill_nan_for_nulls(data_frame[level])

    return data_frame[level].fillna(NULL_VALUE)


def _fill_nan_for_nulls(df):
    """
    Fills the first NaN with a literal string "null" if there are two NaN values, otherwise nothing is filled.

    :param df:
    :return:
    """
    if 1 < pd.isnull(df).sum():
        return df.fillna(NULL_VALUE, limit=1)
    return df
