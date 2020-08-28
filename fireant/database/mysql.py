from pypika import (
    Dialects,
    MySQLQuery,
    Table,
    enums,
    functions as fn,
    terms,
)
from pypika.terms import CustomFunction, Interval, Parameter

from . import sql_types
from .base import Database
from .type_engine import TypeEngine

_DateFormat = CustomFunction('DATE_FORMAT', ['field', 'date_format'])
_DateSub = CustomFunction('DATE_SUB', ['field', 'interval'])
_MakeDate = CustomFunction('MAKEDATE', ['year', 'day_of_year'])
_Weekday = CustomFunction('WEEKDAY', ['arg'])
_Year = CustomFunction('YEAR', ['arg'])
_Quarter = CustomFunction('QUARTER', ['arg'])
_Timestamp = CustomFunction('TIMESTAMP', ['arg'])


class DateAdd(terms.Function):
    """
    Override for the MySQL specific DateAdd function which expects an interval instead of the date part and interval
    unit e.g. DATE_ADD("date", INTERVAL 1 YEAR)
    """

    def __init__(self, field, interval_term, alias=None):
        super(DateAdd, self).__init__('DATE_ADD', field, interval_term, alias=alias)


class _CustomMySQLInterval(terms.Function):
    """
    The PyPika Interval function generates Interval SQL based on the provided time unit arguments.
    Fireant needs a more flexible Interval query string generator than this
    to mimicking the Vertica TRUNC function, so this is defined locally here.
    """
    def __init__(self, field, unit):
        super().__init__('INTERVAL', field, unit)
        self.field = field
        self.unit = unit

    def get_sql(self, **kwargs):
        field_sql = self.field.get_sql(**kwargs)
        return f'INTERVAL {field_sql} {self.unit}'


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
        self.type_engine = MySQLTypeEngine()

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
        if interval == 'hour':
            return _DateFormat(field, '%Y-%m-%d %H:00:00')
        elif interval == 'day':
            return _DateFormat(field, '%Y-%m-%d 00:00:00')
        elif interval == 'week':
            return _DateFormat(_DateSub(field, _CustomMySQLInterval(_Weekday(field), 'DAY')), '%Y-%m-%d 00:00:00')
        elif interval == 'month':
            return _DateFormat(field, '%Y-%m-01')
        elif interval == 'quarter':
            return _MakeDate(_Year(_Timestamp(field)), 1) \
                   + _CustomMySQLInterval(_Quarter(_Timestamp(field)), 'QUARTER') \
                   - Interval(quarters=1, dialect=Dialects.MYSQL)
        elif interval == 'year':
            return _DateFormat(field, '%Y-01-01')
        else:
            raise ValueError(f'Invalid interval provided to trunc_date method: {interval}')

    def to_char(self, definition):
        return fn.Cast(definition, enums.SqlTypes.CHAR)

    def date_add(self, field, date_part, interval):
        # adding an extra 's' as MySQL's interval doesn't work with 'year', 'week' etc, it expects a plural
        interval_term = terms.Interval(**{'{}s'.format(str(date_part)): interval, 'dialect': Dialects.MYSQL})
        return DateAdd(field, interval_term)

    def get_column_definitions(self, schema, table, connection=None):
        columns = Table('columns', schema='INFORMATION_SCHEMA')

        columns_query = MySQLQuery \
            .from_(columns) \
            .select(columns.column_name, columns.column_type) \
            .where(columns.table_schema == Parameter('%(schema)s')) \
            .where(columns.field('table_name') == Parameter('%(table)s')) \
            .distinct() \
            .orderby(columns.column_name)

        return self.fetch(str(columns_query), parameters=dict(schema=schema, table=table), connection=connection)

    def import_csv(self, table, file_path, connection=None):
        """
        Execute a query to import a file into a table using the provided connection.

        :param table: The name of a table to import data into.
        :param file_path: The path of the file to be imported.
        :param connection: (Optional) The connection to execute this query with.
        """
        import_query = MySQLQuery \
            .load(file_path) \
            .into(table)

        self.execute(str(import_query), connection=connection)

    def create_temporary_table_from_columns(self, table, columns, connection=None):
        """
        Creates a temporary table from a list of columns.

        :param table: The name of the new temporary table.
        :param columns: The columns of the new temporary table.
        :param connection: (Optional) The connection to execute this query with.
        """
        create_query = MySQLQuery \
            .create_table(table) \
            .temporary() \
            .columns(*columns)

        self.execute(str(create_query), connection=connection)

    def create_temporary_table_from_select(self, table, select_query, connection=None):
        """
        Creates a temporary table from a SELECT query.

        :param table: The name of the new temporary table.
        :param select_query: The query to be used for selecting data of an existing table for the new temporary table.
        :param connection: (Optional) The connection to execute this query with.
        """
        create_query = MySQLQuery \
            .create_table(table) \
            .temporary() \
            .as_select(select_query)

        self.execute(str(create_query), connection=connection)


class MySQLTypeEngine(TypeEngine):
    mysql_to_ansi_mapper = {
        'bit': sql_types.Char,
        'char': sql_types.Char,
        'nchar': sql_types.Char,
        'varchar': sql_types.VarChar,
        'nvarchar': sql_types.VarChar,
        'text': sql_types.Text,
        'boolean': sql_types.Boolean,
        'int': sql_types.Integer,
        'integer': sql_types.Integer,
        'year': sql_types.Integer,
        'smallint': sql_types.SmallInt,
        'tinyint': sql_types.SmallInt,
        'bigint': sql_types.BigInt,
        'decimal': sql_types.Decimal,
        'fixed': sql_types.Decimal,
        'numeric': sql_types.Numeric,
        'float': sql_types.Float,
        'real': sql_types.DoublePrecision,
        'double': sql_types.DoublePrecision,
        'date': sql_types.Date,
        'time': sql_types.Time,
        'datetime': sql_types.DateTime,
        'timestamp': sql_types.Timestamp,
    }

    ansi_to_mysql_mapper = {
        'CHAR': 'char',
        'VARCHAR': 'varchar',
        'TEXT': 'text',
        'BOOLEAN': 'boolean',
        'INTEGER': 'integer',
        'SMALLINT': 'smallint',
        'BIGINT': 'bigint',
        'DECIMAL': 'decimal',
        'NUMERIC': 'numeric',
        'FLOAT': 'float',
        'REAL': 'real',
        'DOUBLEPRECISION': 'real',
        'DATE': 'date',
        'TIME': 'time',
        'TIMESTAMP': 'timestamp',
    }

    def __init__(self):
        super(MySQLTypeEngine, self).__init__(self.mysql_to_ansi_mapper, self.ansi_to_mysql_mapper)
