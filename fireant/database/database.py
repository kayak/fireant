# coding: utf-8

import pandas as pd


class Database(object):
    def connect(self):
        raise NotImplementedError

    def trunc_date(self, field, interval):
        raise NotImplementedError

    def interval(self, years=0, months=0, days=0, hours=0, minutes=0, seconds=0, microseconds=0, quarters=0, weeks=0):
        raise NotImplementedError

    def fetch(self, query):
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def fetch_dataframe(self, query):
        with self.connect() as connection:
            return pd.read_sql(query, connection)
