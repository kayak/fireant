from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pypika import (
    Parameter,
    Table,
    functions as fn,
    terms,
)
from pypika.dialects import SnowflakeQuery

from .base import Database

try:
    from snowflake import connector as snowflake
except:
    pass

IGNORED_SCHEMAS = {'INFORMATION_SCHEMA'}


class Trunc(terms.Function):
    """
    Wrapper for TRUNC function for truncating dates.
    """

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__('TRUNC', field, date_format, alias=alias)


class SnowflakeDatabase(Database):
    """
    Snowflake client.
    """

    # The pypika query class to use for constructing queries
    query_cls = SnowflakeQuery

    DATETIME_INTERVALS = {'hour': 'HH', 'day': 'DD', 'week': 'W', 'month': 'MM', 'quarter': 'Q', 'year': 'Y'}
    _private_key = None

    def __init__(
        self,
        user='snowflake',
        password=None,
        account='snowflake',
        database='snowflake',
        private_key_data=None,
        private_key_password=None,
        region=None,
        warehouse=None,
        **kwags,
    ):
        super(SnowflakeDatabase, self).__init__(database=database, **kwags)
        self.user = user
        self.password = password
        self.account = account
        self.private_key_data = private_key_data
        self.private_key_password = private_key_password
        self.region = region
        self.warehouse = warehouse

    def connect(self):
        import snowflake

        return snowflake.connector.connect(
            database=self.database,
            account=self.account,
            user=self.user,
            password=self.password,
            private_key=self._get_private_key(),
            region=self.region,
            warehouse=self.warehouse,
        )

    def trunc_date(self, field, interval):
        trunc_date_interval = self.DATETIME_INTERVALS.get(str(interval), 'DD')
        return Trunc(field, trunc_date_interval)

    def date_add(self, field, date_part, interval):
        return fn.TimestampAdd(str(date_part), interval, field)

    def _get_private_key(self):
        if self._private_key is None:
            self._private_key = self._load_private_key_data()

        return self._private_key

    def _load_private_key_data(self):
        if self.private_key_data is None:
            return None

        private_key_password = None if self.private_key_password is None else self.private_key_password.encode()

        pkey = serialization.load_pem_private_key(
            self.private_key_data.encode(), private_key_password, backend=default_backend()
        )

        return pkey.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def get_column_definitions(self, schema, table, connection=None):
        columns = Table('COLUMNS', schema='INFORMATION_SCHEMA')

        columns_query = (
            SnowflakeQuery.from_(columns, immutable=False)
            .select(columns.COLUMN_NAME, columns.DATA_TYPE)
            .where(columns.TABLE_SCHEMA == Parameter('%(schema)s'))
            .where(columns.field('TABLE_NAME') == Parameter('%(table)s'))
            .distinct()
            .orderby(columns.column_name)
        )

        return self.fetch(str(columns_query), connection=connection, parameters=dict(schema=schema, table=table))
