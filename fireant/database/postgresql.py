from pypika import (
    Parameter,
    PostgreSQLQuery,
    Table,
    terms,
    Dialects,
)
from pypika.terms import Node
from pypika.utils import format_quotes

from .base import Database


class DateTrunc(terms.Function):
    """
    Wrapper for the PostgreSQL date_trunc function
    """

    def __init__(self, field, date_format, alias=None):
        super(DateTrunc, self).__init__('DATE_TRUNC', date_format, field, alias=alias)


class PostgreSQLTimestamp(Node):
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def get_sql(self, secondary_quote_char: str = "'", **kwargs) -> str:
        formatted_timestamp = format_quotes(self.timestamp.isoformat(), secondary_quote_char)
        return f"TIMESTAMP {formatted_timestamp}"


class PostgresDateAdd(terms.Function):
    def __init__(self, field, date_part, interval):
        wrapped_field = self.wrap_constant(field, PostgreSQLTimestamp)
        if date_part == "quarter":
            date_part = "month"
            interval *= 3

        interval_term = terms.Interval(**{f'{str(date_part)}s': interval, 'dialect': Dialects.POSTGRESQL})
        super().__init__('DATEADD', wrapped_field, interval_term)

    def get_function_sql(self, **kwargs):
        return " + ".join(self.get_arg_sql(arg, **kwargs) for arg in self.args)


class PostgreSQLDatabase(Database):
    """
    PostgreSQL client that uses the psycopg module.
    """

    # The pypika query class to use for constructing queries
    query_cls = PostgreSQLQuery

    def __init__(self, host="localhost", port=5432, database=None, user=None, password=None, **kwargs):
        super().__init__(host, port, database, **kwargs)
        self.user = user
        self.password = password

    def connect(self):
        import psycopg2

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
        return PostgresDateAdd(field, date_part, interval)

    def get_column_definitions(self, schema, table, connection=None):
        columns = Table("columns", schema="information_schema")

        columns_query = (
            PostgreSQLQuery.from_(columns, immutable=False)
            .select(columns.column_name, columns.data_type)
            .where(columns.table_schema == Parameter('%(schema)s'))
            .where(columns.field("table_name") == Parameter('%(table)s'))
            .distinct()
            .orderby(columns.column_name)
        )

        return self.fetch(str(columns_query), parameters=dict(schema=schema, table=table), connection=connection)
