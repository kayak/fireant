from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from pypika import (
    VerticaQuery,
    functions as fn,
    terms,
)
from .base import Database

try:
    from snowflake import connector as snowflake
except:
    pass


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


class SnowflakeDatabase(Database):
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
    _private_key = None

    def __init__(self, user='snowflake', password=None,
                 account='snowflake', database='snowflake',
                 private_key_data=None, private_key_password=None,
                 region=None, warehouse=None,
                 max_processes=1, cache_middleware=None):
        super(SnowflakeDatabase, self).__init__(database=database,
                                                max_processes=max_processes,
                                                cache_middleware=cache_middleware)
        self.user = user
        self.password = password
        self.account = account
        self.private_key_data = private_key_data
        self.private_key_password = private_key_password
        self.region = region
        self.warehouse = warehouse

    def connect(self):
        import snowflake

        return snowflake.connector.connect(database=self.database,
                                           account=self.account,
                                           user=self.user,
                                           password=self.password,
                                           private_key=self._get_private_key(),
                                           region=self.region,
                                           warehouse=self.warehouse)

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

        private_key_password = None \
            if self.private_key_password is None \
            else self.private_key_password.encode()

        pkey = serialization.load_pem_private_key(self.private_key_data.encode(),
                                                  private_key_password,
                                                  backend=default_backend())

        return pkey.private_bytes(encoding=serialization.Encoding.DER,
                                  format=serialization.PrivateFormat.PKCS8,
                                  encryption_algorithm=serialization.NoEncryption())
