from pypika import (
    PostgreSQLQuery,
    functions as fn,
    terms,
)
from .base import Database


class DateTrunc(terms.Function):
    """
    Wrapper for the PostgreSQL date_trunc function
    """

    def __init__(self, field, date_format, alias=None):
        super(DateTrunc, self).__init__('DATE_TRUNC', date_format, field, alias=alias)
        # Setting the fields here means we can access the TRUNC args by name.
        self.field = field
        self.date_format = date_format
        self.alias = alias


class PostgreSQLDatabase(Database):
    """
    PostgreSQL client that uses the psycopg module.
    """

    # The pypika query class to use for constructing queries
    query_cls = PostgreSQLQuery

    def __init__(self, host='localhost', port=5432, database=None,
                 user=None, password=None, max_processes=1, cache_middleware=None):
        super(PostgreSQLDatabase, self).__init__(host, port, database,
                                                 max_processes=max_processes,
                                                 cache_middleware=cache_middleware)
        self.user = user
        self.password = password

    def connect(self):
        import psycopg2

        return psycopg2.connect(host=self.host, port=self.port, dbname=self.database,
                                user=self.user, password=self.password)

    def trunc_date(self, field, interval):
        return DateTrunc(field, str(interval))

    def date_add(self, field, date_part, interval):
        return fn.DateAdd(str(date_part), interval, field)
