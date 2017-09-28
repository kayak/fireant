# coding: utf-8
from pypika import (
    VerticaQuery,
    functions as fn,
    terms,
)

from fireant.database import Database
from fireant.slicer import DatetimeInterval


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
        'hour': DatetimeInterval('HH'),
        'day': DatetimeInterval('DD'),
        'week': DatetimeInterval('IW'),
        'month': DatetimeInterval('MM'),
        'quarter': DatetimeInterval('Q'),
        'year': DatetimeInterval('Y')
    }

    def __init__(self, host='localhost', port=5433, database='vertica',
                 user='vertica', password=None,
                 read_timeout=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.read_timeout = read_timeout

    def connect(self):
        import vertica_python

        return vertica_python.connect(
            host=self.host, port=self.port, database=self.database,
            user=self.user, password=self.password,
            read_timeout=self.read_timeout,
        )

    def trunc_date(self, field, interval):
        interval = self.DATETIME_INTERVALS[interval]
        return Trunc(field, interval.size)

    def date_add(self, date_part, interval, field):
        return fn.TimestampAdd(date_part, interval, field)


def Vertica(*args, **kwargs):
    from warnings import warn
    warn('The Vertica class is now deprecated. Please use VerticaDatabase instead!')
    return VerticaDatabase(*args, **kwargs)
