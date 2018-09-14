import pandas as pd
from pypika import (
    Query,
    enums,
    functions as fn,
    terms,
)


class Database(object):
    """
    WRITEME
    """
    # The pypika query class to use for constructing queries
    query_cls = Query

    slow_query_log_min_seconds = 15

    def connect(self):
        raise NotImplementedError

    def trunc_date(self, field, interval):
        raise NotImplementedError

    def date_add(self, field: terms.Term, date_part: str, interval: int):
        """ Database specific function for adding or subtracting dates """
        raise NotImplementedError

    def fetch(self, query):
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def to_char(self, definition):
        return fn.Cast(definition, enums.SqlTypes.VARCHAR)

    def fetch_data(self, query):
        with self.connect() as connection:
            return pd.read_sql(query, connection, coerce_float=True, parse_dates=True)
