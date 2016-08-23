# coding: utf-8
import numpy as np
import pandas as pd

from . import Transformer


class PlotlyTransformer(Transformer):
    # FIXME Unfinished

    def transform(self, data_frame, display_schema):
        if isinstance(data_frame.index, pd.MultiIndex):
            data_frame = self._reorder_index_levels(data_frame, display_schema)

        dim_ordinal = {name: ordinal
                       for ordinal, name in enumerate(data_frame.index.names)}
        return self._prepare_data_frame(data_frame, dim_ordinal, display_schema['dimensions'])

    def _prepare_data_frame(self, data_frame, dim_ordinal, dimensions):
        # Replaces invalid values and unstacks the data frame for line charts.

        # Force all fields to be float (Safer for highcharts)
        data_frame = data_frame.astype(np.float).replace([np.inf, -np.inf], np.nan)

        # Unstack multi-indices
        if 1 < len(dimensions):
            # We need to unstack all of the dimensions here after the first dimension, which is the first dimension in
            # the dimensions list, not necessarily the one in the dataframe
            unstack_levels = list(self._unstack_levels(dimensions[1:], dim_ordinal))
            data_frame = data_frame.unstack(level=unstack_levels)

        return data_frame

    def _unstack_levels(self, dimensions, dim_ordinal):
        for dimension in dimensions:
            for id_field in dimension['id_fields']:
                yield dim_ordinal[id_field]

            if 'display_field' in dimension:
                yield dim_ordinal[dimension['display_field']]

    def _reorder_index_levels(self, data_frame, display_schema):
        dimension_orders = [id_field
                            for d in display_schema['dimensions']
                            for id_field in
                            (d['id_fields'] + (
                                [d['display_field']]
                                if 'display_field' in d
                                else []))]
        reordered = data_frame.reorder_levels(data_frame.index.names.index(level)
                                              for level in dimension_orders)
        return reordered


class PandasTransformer(Transformer):
    # FIXME Unfinished

    def transform(self, data_frame, display_schema):
        return data_frame.plot()
