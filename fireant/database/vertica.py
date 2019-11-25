from pypika import (
    Table,
    VerticaQuery,
    functions as fn,
    terms,
)

from .base import Database
from .type_engine import TypeEngine

from .sql_types import (
    Char,
    VarChar,
    Text,
    Boolean,
    Integer,
    SmallInt,
    BigInt,
    Decimal,
    Numeric,
    Float,
    Real,
    DoublePrecision,
    Date,
    Time,
    DateTime,
    Timestamp,
)


class Trunc(terms.Function):
    """
    Wrapper for Vertica TRUNC function for truncating dates.
    """

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__('TRUNC', field, date_format, alias=alias)
        # Setting the fields here means we can access the TRUNC args by name.
        self.field = field
        self.date_format = date_format
        self.alias = alias


class VerticaDatabase(Database):
    """
    Vertica client that uses the vertica_python driver.
    """

    # The pypika query class to use for constructing queries
    query_cls = VerticaQuery

    DATETIME_INTERVALS = {
        'hour': 'HH',
        'day': 'DD',
        'week': 'IW',
        'month': 'MM',
        'quarter': 'Q',
        'year': 'Y'
    }

    def __init__(self, host='localhost', port=5433, database='vertica', user='vertica', password=None,
                 read_timeout=None, **kwags):
        super(VerticaDatabase, self).__init__(host, port, database, **kwags)
        self.user = user
        self.password = password
        self.read_timeout = read_timeout
        self.type_engine = VerticaTypeEngine()

    def connect(self):
        import vertica_python

        return vertica_python.connect(host=self.host, port=self.port, database=self.database,
                                      user=self.user, password=self.password,
                                      read_timeout=self.read_timeout,
                                      unicode_error='replace')

    def trunc_date(self, field, interval):
        trunc_date_interval = self.DATETIME_INTERVALS.get(str(interval), 'DD')
        return Trunc(field, trunc_date_interval)

    def date_add(self, field, date_part, interval):
        return fn.TimestampAdd(str(date_part), interval, field)

    def get_column_definitions(self, schema, table):
        """
        Return schema information including column names and their data type

        :param schema: the name of the schema if you would like to narrow the results down (String)
        :param table: the name of the table if you would like to narrow the results down (String)
        :return: List of column name, column data type pairs
        """
        table_columns = Table('columns')

        table_query = VerticaQuery.from_(table_columns) \
            .select(table_columns.column_name, table_columns.data_type) \
            .where((table_columns.table_schema == schema) & (table_columns.field('table_name') == table)) \
            .distinct()

        return self.fetch(str(table_query))

    def import_csv(self, table, file_path):
        """
        Imports a file into a database table.

        :param table: The name of a table to import data into.
        :param file_path: The path of the file to be imported.
        """
        import_query = VerticaQuery \
            .from_file(file_path) \
            .copy_(table)

        self.execute(str(import_query))

    def create_temporary_table_from_columns(self, table, columns):
        """
        Creates a temporary table from a list of columns.

        :param table: The name of the new temporary table.
        :param columns: The columns of the new temporary table.
        """
        create_query = VerticaQuery \
            .create_table(table) \
            .temporary() \
            .local() \
            .preserve_rows() \
            .columns(*columns)

        self.execute(str(create_query))

    def create_temporary_table_from_select(self, table, select_query):
        """
        Creates a temporary table from a SELECT query.

        :param table: The name of the new temporary table.
        :param select_query: The query to be used for selecting data of an existing table for the new temporary table.
        :return:
        """
        create_query = VerticaQuery \
            .create_table(table) \
            .temporary() \
            .local() \
            .preserve_rows() \
            .as_select(select_query)

        self.execute(str(create_query))


class VerticaTypeEngine(TypeEngine):
    vertica_to_ansi_mapper = {
        'char': Char,
        'varchar': VarChar,
        'varchar2': VarChar,
        'longvarchar': Text,
        'boolean': Boolean,
        'int': Integer,
        'integer': Integer,
        'int8': Integer,
        'smallint': SmallInt,
        'tinyint': SmallInt,
        'bigint': BigInt,
        'decimal': Decimal,
        'numeric': Numeric,
        'number': Numeric,
        'float': Float,
        'float8': Float,
        'real': Real,
        'double': DoublePrecision,
        'date': Date,
        'time': Time,
        'timetz': Time,
        'datetime': DateTime,
        'smalldatetime': DateTime,
        'timestamp': Timestamp,
        'timestamptz': Timestamp,
    }

    ansi_to_vertica_mapper = {
        'CHAR': 'char',
        'VARCHAR': 'varchar',
        'TEXT': 'longvarchar',
        'BOOLEAN': 'boolean',
        'INTEGER': 'integer',
        'SMALLINT': 'smallint',
        'BIGINT': 'bigint',
        'DECIMAL': 'decimal',
        'NUMERIC': 'numeric',
        'FLOAT': 'float',
        'REAL': 'real',
        'DOUBLEPRECISION': 'double',
        'DATE': 'date',
        'TIME': 'time',
        'DATETIME': 'datetime',
        'TIMESTAMP': 'timestamp',
    }

    def __init__(self):
        super(VerticaTypeEngine, self).__init__(self.vertica_to_ansi_mapper, self.ansi_to_vertica_mapper)
