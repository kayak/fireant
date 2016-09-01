# coding: utf-8
import pandas as pd


def cumulative(dataframe, key, metrics, func):
    multilevel = isinstance(dataframe.columns, pd.MultiIndex)
    for metric in metrics:
        if multilevel:
            keys = [((ref, metric), (ref, '{}_{}'.format(metric, key)))
                    for ref in dataframe.columns.levels[0]]

        else:
            keys = [(metric, '{}_{}'.format(metric, key))]

        for old_key, new_key in keys:
            dataframe[new_key] = (
                dataframe[old_key].groupby(level=list(range(1, len(dataframe.index.levels)))).apply(func)
                if isinstance(dataframe.index, pd.MultiIndex)
                else func(dataframe[old_key])
            )

    return dataframe


operations = {
    'cumsum': lambda dataframe, schema: cumulative(dataframe.copy(), 'cumsum', schema['metrics'],
                                                   lambda x: x.expanding(min_periods=1).sum()),
    'cummean': lambda dataframe, schema: cumulative(dataframe.copy(), 'cummean', schema['metrics'],
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
