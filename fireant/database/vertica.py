from pypika import (
    Table,
    VerticaQuery,
    functions as fn,
    terms,
)

from .base import Database


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

    @staticmethod
    def import_csv(connection, table_name, file_path):
        """
        Execute a query to import a file into a table using the provided connection.

        :param connection: The connection for vertica.
        :param table_name: The name of a table to import data into.
        :param file_path: The path of the file to be imported.
        """
        query = VerticaQuery \
            .from_file(file_path) \
            .copy_(table_name)

        cursor = connection.cursor()
        cursor.execute(str(query))

        connection.commit()
