# coding: utf-8
import pandas as pd


def get_cum_metric(dataframe, schema, reference=None):
    metric = schema['metric']
    if reference is None:
        return dataframe[metric]
    return dataframe[reference, metric]


def get_loss_metric(dataframe, schema, reference=None):
    target, metric = (
        (schema['target'], schema['metric'])
        if reference is None
        else ((reference, schema['target']), (reference, schema['metric']))
    )

    error_series = dataframe[target] - dataframe[metric]
    error_series.name = (schema['metric']
                         if reference is None
                         else (reference, schema['metric']))
    return error_series


FUNCTIONS = {
    'cumsum': (get_cum_metric, lambda x: x.expanding(min_periods=1).sum()),
    'cummean': (get_cum_metric, lambda x: x.expanding(min_periods=1).mean()),
    'l1loss': (get_loss_metric, lambda x: x.abs().expanding(min_periods=1).mean()),
    'l2loss': (get_loss_metric, lambda x: x.pow(2).expanding(min_periods=1).mean()),
}


class OperationManager(object):
    def post_process(self, dataframe, operation_schema):
        dataframe = dataframe.copy()

        for schema in operation_schema:
            key = schema['key']

            value_func, operation_func = FUNCTIONS.get(schema['key'])
            if not value_func or not operation_func:
                continue

            self._perform_operation(dataframe, key, schema, value_func, operation_func)

        return dataframe

    def _perform_operation(self, dataframe, key, schema, value_func, operation):
        # Check for references
        references = (dataframe.columns.get_level_values(0).tolist()
                      if isinstance(dataframe.columns, pd.MultiIndex)
                      else [None])

        for reference in references:
            metric_df = value_func(dataframe, schema, reference=reference)

            operation_key = ('{}_{}'.format(metric_df.name, key)
                             if reference is None
                             else (reference, '{}_{}'.format(metric_df.name[1], key)))

            if isinstance(dataframe.index, pd.MultiIndex):
                unstack_levels = list(range(1, len(dataframe.index.levels)))
                dataframe[operation_key] = metric_df.groupby(level=unstack_levels).apply(operation)

            else:
                dataframe[operation_key] = operation(metric_df)
