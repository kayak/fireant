import time

from ...database.base import Database
from typing import Iterable

from .logger import logger


def fetch_data(database: Database, query: str, index_levels: Iterable[str]):
    """
    Returns a Pandas Dataframe built from the result of the query.
    The query is also logged along with its duration.

    :param database:
        instance of fireant.Database, database middleware
    :param query: Query string
    :param index_levels: A list of dimension keys, used for setting the index on the result data frame.

    :return: pd.DataFrame constructed from the result of the query
    """
    start_time = time.time()
    logger.debug(query)

    dataframe = database.fetch_data(str(query))

    logger.info('[{duration} seconds]: {query}'
                .format(duration=round(time.time() - start_time, 4),
                        query=query))

    return dataframe.set_index(index_levels)
