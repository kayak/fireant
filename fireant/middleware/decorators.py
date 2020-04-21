import time
from functools import wraps

from fireant.middleware.slow_query_logger import (
    query_logger,
    slow_query_logger,
)


def log_middleware(func):

    @wraps(func)
    def wrapper(database, *queries, **kwargs):
        results = []

        for query in queries:
            start_time = time.time()
            query_logger.debug(query)

            results.append(func(database, query, **kwargs)[0])

            duration = round(time.time() - start_time, 4)
            query_log_msg = '[{duration} seconds]: {query}'.format(duration=duration,
                                                                   query=query)
            query_logger.info(query_log_msg)

            if database.slow_query_log_min_seconds is not None and duration >= database.slow_query_log_min_seconds:
                slow_query_logger.warning(query_log_msg)

        return results

    return wrapper


def connection_middleware(func):

    @wraps(func)
    def wrapper(database, *queries, **kwargs):
        connection = kwargs.pop('connection', None)
        if connection:
            return func(database, *queries, connection=connection, **kwargs)
        with database.connect() as connection:
            return func(database, *queries, connection=connection, **kwargs)

    return wrapper


def apply_middlewares(wrapped_func):

    @wraps(wrapped_func)
    def wrapper(database, *args, **kwargs):
        func = wrapped_func
        for middleware in reversed(database.middlewares):
            func = middleware(func)

        return func(database, *args, **kwargs)

    return wrapper
