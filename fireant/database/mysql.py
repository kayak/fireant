from pypika import (
    Dialects,
    MySQLQuery,
    Table,
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
                 user=None, password=None, charset='utf8mb4', **kwags):
        super(MySQLDatabase, self).__init__(host, port, database, **kwags)
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

    def get_column_definitions(self, schema, table):
        """ Return a list of column name, column data type pairs """
        columns = Table('columns', schema='INFORMATION_SCHEMA')

        columns_query = MySQLQuery \
            .from_(columns) \
            .select(columns.column_name, columns.column_type) \
            .where(columns.table_schema == schema) \
            .where(columns.field('table_name') == table) \
            .distinct() \
            .orderby(columns.column_name)

        return self.fetch(str(columns_query))

    @staticmethod
    def import_csv(connection, table_name, file_path):
        """
        Execute a query to import a file into a table using the provided connection.

        :param connection: The connection for mysql.
        :param table_name: The name of a table to import data into.
        :param file_path: The path of the file to be imported.
        """
        query = MySQLQuery \
            .load(file_path) \
            .into(table_name)

        cursor = connection.cursor()
        cursor.execute(str(query))

        connection.commit()
