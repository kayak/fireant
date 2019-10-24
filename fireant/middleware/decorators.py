import time
from functools import wraps

from fireant.middleware.slow_query_logger import (
    query_logger,
    slow_query_logger,
)


def db_cache(func):
    @wraps(func)
    def wrapper(database, query):
        if database.cache_middleware is not None:
            return database.cache_middleware(func)(database, query)
        return func(database, query)

    return wrapper


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

        if database.slow_query_log_min_seconds is not None and duration >= database.slow_query_log_min_seconds:
            slow_query_logger.warning(query_log_msg)

        return result

    return wrapper
