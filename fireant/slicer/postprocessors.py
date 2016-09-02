# coding: utf-8
import pandas as pd


def cumulative(dataframe, key, metric, func):
    if isinstance(dataframe.columns, pd.MultiIndex):
        keys = [((ref, metric), (ref, '{}_{}'.format(metric, key)))
                for ref in dataframe.columns.levels[0]]

    else:
        keys = [(metric, '{}_{}'.format(metric, key))]

    for metric_key, operation_key in keys:
        dataframe[operation_key] = (
            dataframe[metric_key].groupby(level=list(range(1, len(dataframe.index.levels)))).apply(func)
            if isinstance(dataframe.index, pd.MultiIndex)
            else func(dataframe[metric_key])
        )

    return dataframe


operations = {
    'cumsum': lambda dataframe, schema: cumulative(dataframe.copy(), 'cumsum', schema['metric'],
                                                   lambda x: x.expanding(min_periods=1).sum()),
    'cummean': lambda dataframe, schema: cumulative(dataframe.copy(), 'cummean', schema['metric'],
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
