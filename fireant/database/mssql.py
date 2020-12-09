from datetime import datetime

from pypika import CustomFunction, MSSQLQuery, Parameter, Table
from pypika.functions import Cast, DateDiff
from pypika.terms import Function, PseudoColumn

from .base import Database

_MSSQLDateAdd = CustomFunction('DATEADD', ['date_part', 'number', 'term'])
_MSSQLConvert = CustomFunction('CONVERT', ['term', 'expression', 'style'])


class MSSQLDatabase(Database):
    """
    Microsoft SQL Server client that uses the pymssql module under the hood.
    """

    # The pypika query class to use for constructing queries
    query_cls = MSSQLQuery

    def __init__(self, host='localhost', port=1433, database=None, user=None, password=None, **kwargs):
        super().__init__(host, port, database, **kwargs)
        self.user = user
        self.password = password

    def connect(self):
        import pymssql

        return pymssql.connect(
            server=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def trunc_date(self, field, interval):
        # Useful docs on this here: http://www.silota.com/docs/recipes/sql-server-date-parts-truncation.html
        return self.date_add(0, interval, DateDiff(PseudoColumn(interval), 0, field))

    def date_add(self, field, date_part, interval):
        return _MSSQLDateAdd(PseudoColumn(date_part), interval, field)

    def convert_date(self, dt: datetime) -> Function:
        return Cast(dt, 'datetimeoffset')

    def get_column_definitions(self, schema, table, connection=None):
        columns = Table('COLUMNS', schema='INFORMATION_SCHEMA')

        columns_query = (
            MSSQLQuery.from_(columns, immutable=False)
            .select(columns.COLUMN_NAME, columns.DATA_TYPE)
            .where(columns.TABLE_SCHEMA == Parameter('%(schema)s'))
            .where(columns.field('TABLE_NAME') == Parameter('%(table)s'))
            .distinct()
            .orderby(columns.column_name)
        )

        return self.fetch(str(columns_query), connection=connection, parameters=dict(schema=schema, table=table))
