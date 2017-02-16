# coding: utf-8
from pypika import terms
from fireant.database import Database
from fireant.database.vertica import Interval


class Trunc(terms.Function):
    # Wrapper for Vertica TRUNC function for truncating dates.

    def __init__(self, field, date_format, alias=None):
        super(Trunc, self).__init__('TRUNC', field, date_format, alias=alias)


class TestDatabase(Database):
    # Vertica client that uses the vertica_python driver.

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def trunc_date(self, field, interval):
        return Trunc(field, interval)

    def interval(self, **kwargs):
        return Interval(**kwargs)
