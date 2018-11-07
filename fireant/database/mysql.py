from pypika import (
    Dialects,
    MySQLQuery,
    enums,
    functions as fn,
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

    def __init__(self, host='localhost', port=3306, database=None,
                 user=None, password=None, charset='utf8mb4', max_processes=1, cache_middleware=None):
        super(MySQLDatabase, self).__init__(host, port, database,
                                            max_processes=max_processes,
                                            cache_middleware=cache_middleware)
        self.user = user
        self.password = password
        self.charset = charset

    def _get_connection_class(self):
        # Nesting inside a function so the import does not cause issues if users have not installed the 'mysql' extra
        # when installing
        import pymysql

        class MySQLConnection(pymysql.connections.Connection):
            """
            PyMySQL has deprecated context managers in the connection class.
            To make the functionality consistent with other database drivers, we override
            the context manager to return the connection instead of the cursor object.

            This also fixes an issue where connections were not being closed in the current PyMySQL
            context manager implementation!
            https://github.com/PyMySQL/PyMySQL/issues/735
            """

            def __enter__(self):
                return self

            def __exit__(self, exc, value, traceback):
                self.close()

        return MySQLConnection

    def connect(self):
        """
        Returns a MySQL connection

        :return: pymysql Connection class
        """
        import pymysql
        connection_class = self._get_connection_class()
        return connection_class(host=self.host, port=self.port, db=self.database,
                                user=self.user, password=self.password,
                                charset=self.charset, cursorclass=pymysql.cursors.Cursor)

    def trunc_date(self, field, interval):
        return Trunc(field, str(interval))

    def to_char(self, definition):
        return fn.Cast(definition, enums.SqlTypes.CHAR)

    def date_add(self, field, date_part, interval):
        # adding an extra 's' as MySQL's interval doesn't work with 'year', 'week' etc, it expects a plural
        interval_term = terms.Interval(**{'{}s'.format(str(date_part)): interval, 'dialect': Dialects.MYSQL})
        return DateAdd(field, interval_term)
