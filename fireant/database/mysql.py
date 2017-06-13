# coding: utf-8
import pandas as pd
from pypika import (MySQLQuery,
                    terms)
from pypika.terms import Interval

from fireant.database import Database


class Trunc(terms.Function):
    """
    Wrapper for a custom MySQL TRUNC function (installed via a custon FireAnt MySQL script
    """

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__('dashmore.TRUNC', field, date_format, alias=alias)


class MySQLDatabase(Database):
    """
    MySQL client that uses the PyMySQL module.
    """
    # The pypika query class to use for constructing queries
    query_cls = MySQLQuery

    def __init__(self, database=None, host='localhost', port=3306,
                 user=None, password=None, charset='utf8mb4'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.charset = charset

    def connect(self):
        import pymysql

        return pymysql.connect(
            host=self.host, port=self.port, db=self.database,
            user=self.user, password=self.password,
            charset=self.charset,
            cursorclass=pymysql.cursors.Cursor,
        )

    def fetch(self, query):
        with self.connect().cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def fetch_dataframe(self, query):
        return pd.read_sql(query, self.connect())

    def trunc_date(self, field, interval):
        return Trunc(field, interval)

    def interval(self, **kwargs):
        return Interval(**kwargs)
