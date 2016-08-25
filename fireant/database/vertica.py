# coding: utf-8
from pypika import terms
from . import Database


class Round(terms.Function):
    # Wrapper for Vertica ROUND function for rounding dates.

    def __init__(self, field, date_format, alias=None):
        super(Round, self).__init__('ROUND', field, date_format, alias=alias)


class Vertica(Database):
    # Vertica client that uses the vertica_python driver.

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

    def round_date(self, field, interval):
        return Round(field, interval)
