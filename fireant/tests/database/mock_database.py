# coding: utf-8
from pypika import terms

from fireant.database import Database


class Round(terms.Function):
    # Wrapper for Vertica ROUND function for rounding dates.

    def __init__(self, field, date_format, alias=None):
        super(Round, self).__init__('ROUND', field, date_format, alias=alias)


class TestDatabase(Database):
    # Vertica client that uses the vertica_python driver.

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def round_date(self, field, interval):
        return Round(field, interval)
