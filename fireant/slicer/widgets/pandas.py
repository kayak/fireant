import pandas as pd

from fireant import Metric
from fireant.utils import format_key
from .base import (
    TransformableWidget,
)
from ..references import (
    reference_key,
    reference_label,
)
from ... import formats

HARD_MAX_COLUMNS = 24


class Pandas(TransformableWidget):
    def __init__(self, metric, *metrics: Metric, pivot=False, max_columns=None):
        super(Pandas, self).__init__(metric, *metrics)
        self.pivot = pivot
        self.max_columns = min(max_columns, HARD_MAX_COLUMNS) \
            if max_columns is not None \
            else HARD_MAX_COLUMNS

    def transform(self, data_frame, slicer, dimensions, references):
        """
        WRITEME

        :param data_frame:
        :param slicer:
        :param dimensions:
        :param references:
        :return:
        """
        result = data_frame.copy()

        for metric in self.items:
            if any([metric.precision is not None,
                    metric.prefix is not None,
                    metric.suffix is not None]):
                df_key = format_key(metric.key)

                result[df_key] = result[df_key] \
                    .apply(lambda x: formats.metric_display(x, metric.prefix, metric.suffix, metric.precision))

        for dimension in dimensions:
            if dimension.has_display_field:
                result = result.set_index(format_key(dimension.display_key), append=True)
                result = result.reset_index(format_key(dimension.key), drop=True)

            if hasattr(dimension, 'display_values'):
                self._replace_display_values_in_index(dimension, result)

        if isinstance(data_frame.index, pd.MultiIndex):
            index_levels = [format_key(dimension.display_key)
                            if dimension.has_display_field
                            else format_key(dimension.key)
                            for dimension in dimensions]

            result = result.reorder_levels(index_levels)

        result = result[[format_key(reference_key(item, reference))
                         for reference in [None] + references
                         for item in self.items]]

        if dimensions:
            result.index.names = [dimension.label or dimension.key
                                  for dimension in dimensions]

        result.columns = [reference_label(item, reference)
                          for reference in [None] + references
                          for item in self.items]

        if not self.pivot:
            return result

        pivot_levels = result.index.names[1:]
        return result.unstack(level=pivot_levels)

    def _replace_display_values_in_index(self, dimension, result):
        """
        Replaces the raw values of a (categorical) dimension in the index with their corresponding display values.
        """
        if isinstance(result.index, pd.MultiIndex):
            df_key = format_key(dimension.key)
            values = [dimension.display_values.get(x, x)
                      for x in result.index.get_level_values(df_key)]
            result.index.set_levels(level=df_key, levels=values)
            return result

        values = [dimension.display_values.get(x, x)
                  for x in result.index]
        result.index = pd.Index(values, name=result.index.name)
        return result
