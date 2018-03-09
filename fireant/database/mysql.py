import pandas as pd

from pypika import (
    Dialects,
    MySQLQuery,
    terms,
)
from .base import Database


class Trunc(terms.Function):
    """
    Wrapper for a custom MySQL TRUNC function (installed via a custom FireAnt MySQL script)
    """

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__('dashmore.TRUNC', field, date_format, alias=alias)
        # Setting the fields here means we can access the TRUNC args by name.
        self.field = field
        self.date_format = date_format
        self.alias = alias


class DateAdd(terms.Function):
    """
    Override for the MySQL specific DateAdd function which expects an interval instead of the date part and interval
    unit e.g. DATE_ADD("date", INTERVAL 1 YEAR)
    """

    def __init__(self, field, interval_term, alias=None):
        super(DateAdd, self).__init__('DATE_ADD', field, interval_term, alias=alias)


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

        return pymysql.connect(host=self.host, port=self.port, db=self.database,
                               user=self.user, password=self.password,
                               charset=self.charset,
                               cursorclass=pymysql.cursors.Cursor)

    def fetch(self, query):
        with self.connect().cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def fetch_data(self, query):
        return pd.read_sql(query, self.connect())

    def trunc_date(self, field, interval):
        return Trunc(field, str(interval))

    def date_add(self, field, date_part, interval):
        # adding an extra 's' as MySQL's interval doesn't work with 'year', 'week' etc, it expects a plural
        interval_term = terms.Interval(**{'{}s'.format(str(date_part)): interval, 'dialect': Dialects.MYSQL})
        return DateAdd(field, interval_term)

    def totals(self, query, terms):
        raise NotImplementedError
