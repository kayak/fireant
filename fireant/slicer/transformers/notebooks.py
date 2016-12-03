# coding: utf-8
import pandas as pd

from fireant import settings
from . import Transformer, TransformationException


def _format_dimension_labels(dimension):
    if 'display_field' in dimension:
        return ['%s ID' % dimension['label'], dimension['label']]

    return [dimension['label']]


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

                if isinstance(dataframe.index, pd.MultiIndex):
                    dataframe.index.set_levels(display_values, key, inplace=True)

                else:
                    dataframe.index = pd.Index(display_values)

        return dataframe

    def _set_dimension_labels(self, dataframe, dimensions):
        dataframe = dataframe.copy()
        dataframe.index.names = [label
                                 for key, dimension in dimensions.items()
                                 for label in _format_dimension_labels(dimension)]

        return dataframe

    def _set_metric_labels(self, dataframe, metrics, references):
        dataframe = dataframe.copy()

        labels = [metric['label'] for metric in metrics.values()]
        if isinstance(dataframe.columns, pd.MultiIndex):
            dataframe.columns = dataframe.columns.reorder_levels((1, 0)) \
                .set_levels([labels,
                             [None] + list(references.values())]) \
                .set_names('Reference', 1)

        else:
            dataframe.columns = labels

        return dataframe

class PandasColumnIndexTransformer(PandasRowIndexTransformer):
    def transform(self, dataframe, display_schema):
        dataframe = super(PandasColumnIndexTransformer, self).transform(dataframe, display_schema)

        if isinstance(dataframe.index, pd.MultiIndex):
            drop_levels = [dimension['label']
                           for dimension in list(display_schema['dimensions'].values())[1:]]

            dataframe = dataframe.unstack(level=drop_levels)

        return dataframe


class MatplotlibLineChartTransformer(PandasColumnIndexTransformer):
    def transform(self, dataframe, display_schema):
        self._validate_dimensions(dataframe, display_schema['dimensions'])
        dataframe = super(MatplotlibLineChartTransformer, self).transform(dataframe, display_schema)

        metrics = list(display_schema['metrics'].values())
        height, width = settings.matplotlib_figsize or (14, 5)
        figsize = (height, width * len(metrics))

        if 1 == len(metrics):
            return dataframe.plot.line(figsize=figsize) \
                .legend(loc='center left', bbox_to_anchor=(1, 0.5)) \
                .set_title(metrics[0]['label'])

        try:
            import matplotlib.pyplot as plt
        except ImportError:
            raise TransformationException('Missing library: matplotlib')

        fig, axes = plt.subplots(len(metrics), sharex=True, figsize=figsize)
        for metric, axis in zip(metrics, axes):
            label = metric['label']
            dataframe[label].plot.line(ax=axis) \
                .legend(loc='center left', bbox_to_anchor=(1, 0.5)) \
                .set_title(label)

        return axes

    def _validate_dimensions(self, dataframe, dimensions):
        if not 0 < len(dimensions):
            raise TransformationException('Cannot transform line chart.  '
                                          'At least one dimension is required.')


class MatplotlibBarChartTransformer(PandasColumnIndexTransformer):
    def transform(self, dataframe, display_schema):
        dataframe = super(MatplotlibBarChartTransformer, self).transform(dataframe, display_schema)

        return dataframe.plot.bar(figsize=settings.matplotlib_figsize or (14, 5)) \
            .legend(loc='center left', bbox_to_anchor=(1, 0.5))
