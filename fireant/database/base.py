from pypika import (
    Query,
    enums,
    functions as fn,
    terms,
)


class Database(object):
    """
    This is a abstract base class used for interfacing with a database platform.
    """

    # The pypika query class to use for constructing queries
    query_cls = Query

    slow_query_log_min_seconds = 15

    def __init__(self, host=None, port=None, database=None, max_processes=2, max_result_set_size=200000,
                 cache_middleware=None):
        self.host = host
        self.port = port
        self.database = database
        self.max_processes = max_processes
        self.max_result_set_size = max_result_set_size
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
