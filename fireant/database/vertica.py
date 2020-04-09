from pypika import (
    Tables,
    VerticaQuery,
    functions as fn,
    terms,
)
from .base import Database
from .sql_types import (
    BigInt,
    Boolean,
    Char,
    Date,
    DateTime,
    Decimal,
    DoublePrecision,
    Float,
    Integer,
    Numeric,
    Real,
    SmallInt,
    Text,
    Time,
    Timestamp,
    VarChar,
)
from .type_engine import TypeEngine


class Trunc(terms.Function):
    """
    Wrapper for Vertica TRUNC function for truncating dates.
    """

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__("TRUNC", field, date_format, alias=alias)
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
        "hour": "HH",
        "day": "DD",
        "week": "IW",
        "month": "MM",
        "quarter": "Q",
        "year": "Y",
    }

    def __init__(
        self,
        host="localhost",
        port=5433,
        database="vertica",
        user="vertica",
        password=None,
        read_timeout=None,
        **kwags
    ):
        super(VerticaDatabase, self).__init__(host, port, database, **kwags)
        self.user = user
        self.password = password
        self.read_timeout = read_timeout
        self.type_engine = VerticaTypeEngine()

    def connect(self):
        import vertica_python

        return vertica_python.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            read_timeout=self.read_timeout,
            unicode_error="replace",
        )

    def trunc_date(self, field, interval):
        trunc_date_interval = self.DATETIME_INTERVALS.get(str(interval), "DD")
        return Trunc(field, trunc_date_interval)

    def date_add(self, field, date_part, interval):
        return fn.TimestampAdd(str(date_part), interval, field)

    def get_column_definitions(self, schema, table, connection=None):
        view_columns, table_columns = Tables('view_columns', 'columns')

        view_query = VerticaQuery.from_(view_columns) \
            .select(view_columns.column_name, view_columns.data_type) \
            .where((view_columns.table_schema == schema) & (view_columns.field('table_name') == table)) \
            .distinct()

        table_query = (
            VerticaQuery.from_(table_columns, immutable=False)
            .select(table_columns.column_name, table_columns.data_type)
            .where(
                (table_columns.table_schema == schema)
                & (table_columns.field("table_name") == table)
            )
            .distinct()
        )

        return self.fetch(str(view_query + table_query), connection=connection)

    def import_csv(self, table, file_path, connection=None):
        """
        Imports a file into a database table.

        :param table: The name of a table to import data into.
        :param file_path: The path of the file to be imported.
        :param connection: (Optional) The connection to execute this query with.
        """
        import_query = VerticaQuery.from_file(file_path).copy_(table)

        self.execute(str(import_query), connection=connection)

    def create_temporary_table_from_columns(self, table, columns, connection=None):
        """
        Creates a temporary table from a list of columns.

        :param table: The name of the new temporary table.
        :param columns: The columns of the new temporary table.
        :param connection: (Optional) The connection to execute this query with.
        """
        create_query = (
            VerticaQuery.create_table(table)
            .temporary()
            .local()
            .preserve_rows()
            .columns(*columns)
        )

        self.execute(str(create_query), connection=connection)

    def create_temporary_table_from_select(self, table, select_query, connection=None):
        """
        Creates a temporary table from a SELECT query.

        :param table: The name of the new temporary table.
        :param select_query: The query to be used for selecting data of an existing table for the new temporary table.
        :param connection: (Optional) The connection to execute this query with.
        """
        create_query = (
            VerticaQuery.create_table(table)
            .temporary()
            .local()
            .preserve_rows()
            .as_select(select_query)
        )

        self.execute(str(create_query), connection=connection)


class VerticaTypeEngine(TypeEngine):
    vertica_to_ansi_mapper = {
        "char": Char,
        "varchar": VarChar,
        "varchar2": VarChar,
        "longvarchar": Text,
        "boolean": Boolean,
        "int": Integer,
        "integer": Integer,
        "int8": Integer,
        "smallint": SmallInt,
        "tinyint": SmallInt,
        "bigint": BigInt,
        "decimal": Decimal,
        "numeric": Numeric,
        "number": Numeric,
        "float": Float,
        "float8": Float,
        "real": Real,
        "double": DoublePrecision,
        "date": Date,
        "time": Time,
        "timetz": Time,
        "datetime": DateTime,
        "smalldatetime": DateTime,
        "timestamp": Timestamp,
        "timestamptz": Timestamp,
    }

    ansi_to_vertica_mapper = {
        "CHAR": "char",
        "VARCHAR": "varchar",
        "TEXT": "longvarchar",
        "BOOLEAN": "boolean",
        "INTEGER": "integer",
        "SMALLINT": "smallint",
        "BIGINT": "bigint",
        "DECIMAL": "decimal",
        "NUMERIC": "numeric",
        "FLOAT": "float",
        "REAL": "real",
        "DOUBLEPRECISION": "double",
        "DATE": "date",
        "TIME": "time",
        "DATETIME": "datetime",
        "TIMESTAMP": "timestamp",
    }

    def __init__(self):
        super(VerticaTypeEngine, self).__init__(
            self.vertica_to_ansi_mapper, self.ansi_to_vertica_mapper
        )
