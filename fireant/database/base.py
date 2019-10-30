import pandas as pd

from pypika import (
    Query,
    enums,
    functions as fn,
    terms,
)

from fireant.middleware.decorators import (
    db_cache,
    log,
)

from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware
from fireant.utils import write_named_temp_csv


class Database(object):
    """
    This is a abstract base class used for interfacing with a database platform.
    """

    # The pypika query class to use for constructing queries
    query_cls = Query

    slow_query_log_min_seconds = 15

    def __init__(self, host=None, port=None, database=None, max_processes=1, max_result_set_size=200000,
                 cache_middleware=None, concurrency_middleware=None):
        self.host = host
        self.port = port
        self.database = database
        self.max_result_set_size = max_result_set_size
        self.cache_middleware = cache_middleware
        self.concurrency_middleware = concurrency_middleware or ThreadPoolConcurrencyMiddleware(max_processes)

    def connect(self):
        """
        This function must establish a connection to the database platform and return it.
        """
        raise NotImplementedError

    def get_column_definitions(self, schema, table):
        """
        This function must return the columns of a given schema and table.
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

    @db_cache
    @log
    def fetch(self, query):
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    @db_cache
    @log
    def fetch_dataframe(self, query):
        with self.connect() as connection:
            return pd.read_sql(query, connection, coerce_float=True, parse_dates=True)

    @staticmethod
    def export_csv(connection, query):
        """
        Export result of query to temporary csv file.

        :param connection: The database connection.
        :param query: The pypika query to be executed.
        :return: A named temporary file containing the query result.
        """
        cursor = connection.cursor()
        cursor.execute(str(query))

        result = cursor.fetchall()

        return write_named_temp_csv(result)
