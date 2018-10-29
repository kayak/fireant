from functools import wraps

import pandas as pd
import time

from pypika import (
    Query,
    enums,
    functions as fn,
    terms,
)
from .logger import (
    query_logger,
    slow_query_logger,
)


def log(func):
    @wraps(func)
    def wrapper(database, query):
        start_time = time.time()
        query_logger.debug(query)

        result = func(database, query)

        duration = round(time.time() - start_time, 4)
        query_log_msg = '[{duration} seconds]: {query}'.format(duration=duration,
                                                               query=query)
        query_logger.info(query_log_msg)

        if duration >= database.slow_query_log_min_seconds:
            slow_query_logger.warning(query_log_msg)

        return result

    return wrapper


@log
def fetch(database, query):
    with database.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()


@log
def fetch_data(database, query):
    with database.connect() as connection:
        return pd.read_sql(query, connection, coerce_float=True, parse_dates=True)


class Database(object):
    """
    This is a abstract base class used for interfacing with a database platform.

    """
    # The pypika query class to use for constructing queries
    query_cls = Query

    slow_query_log_min_seconds = 15

    def __init__(self, host=None, port=None, database=None, max_processes=2, cache_middleware=None):
        self.host = host
        self.port = port
        self.database = database
        self.max_processes = max_processes
        self.cache_middleware = cache_middleware

    def connect(self):
        """
        This function must establish a connection to the database platform and return it.
        """
        raise NotImplementedError

    def trunc_date(self, field, interval):
        """
        This function must create a Pypika function which truncates a Date or DateTime object to a specific interval.
        """
        raise NotImplementedError

    def date_add(self, field: terms.Term, date_part: str, interval: int):
        """
        This function must add/subtract a Date or Date/Time object.
        """
        raise NotImplementedError

    def to_char(self, definition):
        return fn.Cast(definition, enums.SqlTypes.VARCHAR)

    def fetch(self, query):
        if self.cache_middleware is not None:
            return self.cache_middleware(fetch)(self, query)
        return fetch(self, query)

    def fetch_data(self, query):
        if self.cache_middleware is not None:
            return self.cache_middleware(fetch_data)(self, query)

        return fetch_data(self, query)
