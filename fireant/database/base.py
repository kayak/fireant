import pandas as pd

from pypika import (
    Query,
    enums,
    functions as fn,
    terms,
)

from fireant.middleware.decorators import (
    connection_middleware,
    apply_middlewares,
)


class Database(object):
    """
    This is a abstract base class used for interfacing with a database platform.
    """

    # The pypika query class to use for constructing queries
    query_cls = Query

    slow_query_log_min_seconds = 15

    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        max_result_set_size=200000,
        middlewares=[],
    ):
        self.host = host
        self.port = port
        self.database = database
        self.max_result_set_size = max_result_set_size
        self.middlewares = middlewares + [connection_middleware]

    def connect(self):
        """
        This function must establish a connection to the database platform and return it.
        """
        raise NotImplementedError

    def get_column_definitions(self, schema, table, connection=None):
        """
        Return a list of column name, column data type pairs.

        :param schema: The name of the table schema.
        :param table: The name of the table to get columns from.
        :param connection: (Optional) The connection to execute this query with.
        :return: A list of columns.
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

    @apply_middlewares
    def fetch_queries(self, *queries, **kwargs):
        results = []
        connection = kwargs.get("connection")
        for query in queries:
            cursor = connection.cursor()
            cursor.execute(str(query))
            results.append(cursor.fetchall())

        return results

    def fetch(self, query, **kwargs):
        return self.fetch_queries(query, **kwargs)[0]

    @apply_middlewares
    def execute(self, *queries, **kwargs):
        connection = kwargs.get("connection")
        for query in queries:
            cursor = connection.cursor()
            cursor.execute(str(query))
            connection.commit()

    @apply_middlewares
    def fetch_dataframes(self, *queries, parse_dates=None, **kwargs):
        connection = kwargs.get("connection")
        dataframes = []
        for query in queries:
            dataframes.append(
                pd.read_sql(query, connection, coerce_float=True, parse_dates=parse_dates)
            )
        return dataframes

    def fetch_dataframe(self, query, **kwargs):
        return self.fetch_dataframes(query, **kwargs)[0]
