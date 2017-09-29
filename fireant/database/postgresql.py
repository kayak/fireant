# coding: utf-8
import pandas as pd
from pypika import (
    PostgreSQLQuery,
    functions as fn,
    terms,
)

from fireant.database import Database


class Trunc(terms.Function):
    """
    Wrapper for the PostgreSQL date_trunc function
    """

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__('date_trunc', date_format, field, alias=alias)
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

    def __init__(self, database=None, host='localhost', port=5432,
                 user=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        import psycopg2

        return psycopg2.connect(
            host=self.host, port=self.port, dbname=self.database,
            user=self.user, password=self.password,
        )

    def fetch(self, query):
        with self.connect().cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def fetch_dataframe(self, query):
        return pd.read_sql(query, self.connect())

    def trunc_date(self, field, interval):
        return Trunc(field, interval)

    def date_add(self, date_part, interval, field):
        return fn.DateAdd(date_part, interval, field)
