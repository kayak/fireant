import pandas as pd

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
    def __init__(self, items=(), pivot=False, max_columns=None):
        super(Pandas, self).__init__(items)
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
                result[metric.key] = result[metric.key] \
                    .apply(lambda x: formats.metric_display(x, metric.prefix, metric.suffix, metric.precision))

        for dimension in dimensions:
            if dimension.has_display_field:
                result = result.set_index(dimension.display_key, append=True)
                result = result.reset_index(dimension.key, drop=True)

            if hasattr(dimension, 'display_values'):
                self._replace_display_values_in_index(dimension, result)

        if isinstance(data_frame.index, pd.MultiIndex):
            index_levels = [dimension.display_key
                            if dimension.has_display_field
                            else dimension.key
                            for dimension in dimensions]

            result = result.reorder_levels(index_levels)

        result = result[[reference_key(item, reference)
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
            values = [dimension.display_values.get(x, x)
                      for x in result.index.get_level_values(dimension.key)]
            result.index.set_levels(level=dimension.key, levels=values)
            return result

        values = [dimension.display_values.get(x, x)
                  for x in result.index]
        result.index = pd.Index(values, name=result.index.name)
        return result
