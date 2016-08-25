# coding: utf-8
import pandas as pd

from . import Transformer, TransformationException


class PandasRowIndexTransformer(Transformer):
    def transform(self, dataframe, display_schema):
        dataframe = self._set_display_options(dataframe, display_schema)
        if display_schema['dimensions']:
            dataframe = self._set_dimension_labels(dataframe, display_schema['dimensions'])
        dataframe = self._set_metric_labels(dataframe, display_schema['metrics'], display_schema.get('references'))

        if isinstance(dataframe.index, pd.MultiIndex):
            drop_levels = ['%s ID' % dimension['label']
                           for key, dimension in display_schema['dimensions'].items()
                           if dimension.get('display_field')]
            dataframe.reset_index(level=drop_levels, drop=True, inplace=True)

        return dataframe

    def _set_display_options(self, dataframe, display_schema):
        dataframe = dataframe.copy()

        for key, dimension in display_schema['dimensions'].items():
            if 'display_options' in dimension:
                display_values = [dimension['display_options'].get(value, value)
                                  for value in dataframe.index.get_level_values(key).unique()]
                dataframe.index.set_levels(display_values, key, inplace=True)

        return dataframe

    def _set_dimension_labels(self, dataframe, dimensions):
        dataframe = dataframe.copy()
        dataframe.index.names = [label
                                 for key, dimension in dimensions.items()
                                 for label in self._dimension_labels(dimension)]

        return dataframe

    def _set_metric_labels(self, dataframe, metrics, references):
        dataframe = dataframe.copy()

        if isinstance(dataframe.columns, pd.MultiIndex):
            dataframe.columns = dataframe.columns.reorder_levels((1, 0)) \
                .set_levels([list(metrics.values()),
                             [None] + list(references.values())]) \
                .set_names('Reference', 1)

        else:
            dataframe.columns = list(metrics.values())

        return dataframe

    def _dimension_labels(self, dimension):
        if 'display_field' in dimension:
            return ['%s ID' % dimension['label'], dimension['label']]

        return [dimension['label']]


class PandasColumnIndexTransformer(PandasRowIndexTransformer):
    def transform(self, dataframe, display_schema):
        dataframe = super(PandasColumnIndexTransformer, self).transform(dataframe, display_schema)

        if isinstance(dataframe.index, pd.MultiIndex):
            drop_levels = [dimension['label']
                           for dimension in list(display_schema['dimensions'].values())[1:]]

            dataframe = dataframe.unstack(level=drop_levels)

        return dataframe


try:
    import matplotlib.pyplot as plt


    class MatplotlibLineChartTransformer(PandasColumnIndexTransformer):
        def transform(self, dataframe, display_schema):
            self._validate_dimensions(dataframe, display_schema['dimensions'])
            dataframe = super(MatplotlibLineChartTransformer, self).transform(dataframe, display_schema)

            metrics = list(display_schema['metrics'].values())
            figsize = (14, 5 * len(metrics))

            if 1 == len(metrics):
                return dataframe.plot.line(figsize=figsize) \
                    .legend(loc='center left', bbox_to_anchor=(1, 0.5)) \
                    .set_title(metrics[0])

            fig, axes = plt.subplots(len(metrics), sharex=True, figsize=figsize)
            for metric, axis in zip(metrics, axes):
                dataframe[metric].plot.line(ax=axis) \
                    .legend(loc='center left', bbox_to_anchor=(1, 0.5)) \
                    .set_title(metric)

            return axes

        def _validate_dimensions(self, dataframe, dimensions):
            if not 0 < len(dimensions):
                raise TransformationException('Cannot transform line chart.  '
                                              'At least one dimension is required.')


    class MatplotlibBarChartTransformer(PandasColumnIndexTransformer):
        def transform(self, dataframe, display_schema):
            dataframe = super(MatplotlibBarChartTransformer, self).transform(dataframe, display_schema)

            return dataframe.plot.bar(figsize=(14, 5)) \
                .legend(loc='center left', bbox_to_anchor=(1, 0.5))

except ImportError:
    # matplotlib not installed
    pass
