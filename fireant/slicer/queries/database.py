import time

from .logger import logger


def fetch_data(database, query, index_levels):
    """
    Returns a Pandas Dataframe built from the result of the query.
    The query is also logged along with its duration.

    :param database: Database object
    :param query: PyPika query object
    :return: Pandas Dataframe built from the result of the query
    """
    start_time = time.time()
    logger.debug(query)

    dataframe = database.fetch_dataframe(query)

    logger.info('[{duration} seconds]: {query}'
                .format(duration=round(time.time() - start_time, 4),
                        query=query))

    return dataframe.set_index(index_levels)
