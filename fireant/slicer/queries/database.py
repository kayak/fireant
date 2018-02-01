import time
from typing import Iterable

import pandas as pd

from fireant.database.base import Database
from .logger import logger
from ..dimensions import (
    Dimension,
    RollupDimension,
)

CONTINUOUS_DIMS = (pd.DatetimeIndex, pd.RangeIndex)


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

    :param data_frame:
    :param dimensions:
    :return:
    """
    if not dimensions:
        return data_frame

    dimension_keys = [d.key for d in dimensions]

    for i, dimension in enumerate(dimensions):
        if not isinstance(dimension, RollupDimension):
            continue

        level = dimension.key
        if dimension.is_rollup:
            # Rename the first instance of NaN to totals for each dimension value
            # If there are multiple dimensions, we need to group by the preceding dimensions for each dimension
            data_frame[level] = (
                data_frame
                    .groupby(dimension_keys[:i])[level]
                    .fillna('Totals', limit=1)
            ) if 0 < i else (
                data_frame[level]
                    .fillna('Totals', limit=1)
            )

        data_frame[level] = data_frame[level] \
            .fillna('') \
            .astype('str')

    # Set index on dimension columns
    return data_frame.set_index(dimension_keys)
