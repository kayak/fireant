import signal
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


class CancelableConnection:
    """
    This is essentially a context manager that wraps around connection contextmanagers.
    A handler will be attached to the SIGINT signal for cancellation purposes and removed again when exiting
    the context.
    """

    def __init__(self, database, wait_time_after_close=0):
        self.database = database
        self.connection_context_manager = None
        self.connection = None
        self.wait_time_after_close = wait_time_after_close
        self.previous_signal_handler = None

    def __enter__(self):
        """
        self._handle_interrupt_signal gets set as signal handler for SIGINT right after opening the db connection.
        """
        self.previous_signal_handler = signal.getsignal(signal.SIGINT)
        self.connection_context_manager = self.database.connect()
        self.connection = self.connection_context_manager.__enter__()
        signal.signal(signal.SIGINT, self._handle_interrupt_signal)
        return self.connection

    def __exit__(self, exception_type, exception_value, traceback):
        """
        self._handle_interrupt_signal gets removed as signal handler for SIGINT right before closing the db connection.
        """
        signal.signal(signal.SIGINT, self.previous_signal_handler)
        self.connection_context_manager.__exit__(exception_type, exception_value, traceback)
        if self.wait_time_after_close:
            time.sleep(self.wait_time_after_close)

    def _handle_interrupt_signal(self, sig_num, frame):
        """
        On SIGINT we want to cancel any outstanding query.
        """
        self.database.cancel(self.connection)


def connection_middleware(func):

    @wraps(func)
    def wrapper(database, *queries, **kwargs):
        connection = kwargs.pop('connection', None)
        if connection:
            return func(database, *queries, connection=connection, **kwargs)

        with CancelableConnection(database, kwargs.pop("wait_time_after_close", 0)) as connection:
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
