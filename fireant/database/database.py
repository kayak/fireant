# coding: utf-8

import pandas as pd


class Database(object):
    def connect(self):
        raise NotImplementedError

    def trunc_date(self, field, interval):
        raise NotImplementedError

    def fetch(self, query):
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def fetch_dataframe(self, query):
        with self.connect() as connection:
            return pd.read_sql(query, connection)
