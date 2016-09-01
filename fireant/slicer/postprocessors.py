# coding: utf-8
import pandas as pd


class invert_levels():
    def __init__(self, dataframe):
        self.dataframe = dataframe.copy()

    @staticmethod
    def invert_column_levels(dataframe):
        if isinstance(dataframe.columns, pd.MultiIndex):
            dataframe.columns = dataframe.columns.reorder_levels([1, 0])

        return dataframe

    def __enter__(self):
        return self.invert_column_levels(self.dataframe)

    def __exit__(self, type, value, traceback):
        self.invert_column_levels(self.dataframe)


def cumulative(dataframe, metrics, func):
    with invert_levels(dataframe) as dataframe:
        # Cannot apply expanding function directly with groupby
        # https://github.com/pydata/pandas/issues/14013
        for metric in metrics:
            dataframe[metric] = (
                dataframe[metric].groupby(level=list(range(1, len(dataframe.index.levels)))).apply(func)
                if isinstance(dataframe.index, pd.MultiIndex)
                else func(dataframe[metric])
            )

    return dataframe


operations = {
    'cumsum': lambda dataframe, schema: cumulative(dataframe, schema['metrics'],
                                                   lambda x: x.expanding(min_periods=1).sum()),
    'cummean': lambda dataframe, schema: cumulative(dataframe, schema['metrics'],
                                                    lambda x: x.expanding(min_periods=1).mean()),
}


class OperationManager(object):
    def post_process(self, dataframe, operation_schema):

        for schema in operation_schema:
            operation = operations.get(schema['key'])

            if not operation:
                continue

            dataframe = operation(dataframe, schema)

        return dataframe
