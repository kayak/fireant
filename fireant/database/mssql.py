from pypika import (
    MSSQLQuery, functions as fn,
    terms,
)

from .base import Database


class DateTrunc(terms.Function):
    """
    Wrapper for the PostgreSQL date_trunc function
    """

    def __init__(self, field, date_format, alias=None):
        super(DateTrunc, self).__init__('DATE_TRUNC', date_format, field, alias=alias)


class MSSQLDatabase(Database):
    """
    Microsoft SQL Server client that uses the psycopg module.
    """

    # The pypika query class to use for constructing queries
    query_cls = MSSQLQuery

    def __init__(
        self,
        host='localhost',
        port=1433,
        database=None,
        user=None,
        password=None,
        **kwargs
    ):
        super().__init__(host, port, database, **kwargs)
        self.user = user
        self.password = password

    def connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )

    def trunc_date(self, field, interval):
        return DateTrunc(field, str(interval))

    def date_add(self, field, date_part, interval):
        return fn.DateAdd(str(date_part), interval, field)

    def get_column_definitions(self, schema, table, connection=None):
        pass
#         columns = Table("columns", schema="INFORMATION_SCHEMA")
#
#         columns_query = (
#             MSSQLQuery.from_(columns, immutable=False)
#             .select(columns.column_name, columns.data_type)
#             .where(columns.table_schema == schema)
#             .where(columns.field("table_name") == table)
#             .distinct()
#             .orderby(columns.column_name)
#         )
#
#         columns_query = ('SELECT *'
# 'FROM INFORMATION_SCHEMA.COLUMNS''
# 'WHERE TABLE_NAME = N'Customers')
#
#         return self.fetch(str(columns_query), connection=connection)
