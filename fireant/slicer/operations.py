import numpy as np
import pandas as pd

from fireant.slicer.references import reference_key
from fireant.slicer.totals import get_totals_marker_for_dtype
from fireant.utils import (
    format_dimension_key,
    format_metric_key,
    reduce_data_frame_levels,
)
from .dimensions import Dimension
from .metrics import Metric


def _extract_key_or_arg(data_frame, key):
    return data_frame[key] \
        if key in data_frame \
        else key


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` API.
    """

    def apply(self, data_frame, reference):
        raise NotImplementedError()

    @property
    def metrics(self):
        raise NotImplementedError()

    @property
    def operations(self):
        return []


class _BaseOperation(Operation):
    def __init__(self, key, label, prefix=None, suffix=None, precision=None):
        self.key = key
        self.label = label
        self.prefix = prefix
        self.suffix = suffix
        self.precision = precision

    def apply(self, data_frame, reference):
        raise NotImplementedError()

    @property
    def metrics(self):
        raise NotImplementedError()

    @property
    def operations(self):
        raise NotImplementedError()

    def _group_levels(self, index):
        """
        Get the index levels that need to be grouped. This is to avoid apply the cumulative function across separate
        dimensions. Only the first dimension should be accumulated across.

        :param index:
        :return:
        """
        return index.names[1:]


class _Cumulative(_BaseOperation):
    def __init__(self, arg):
        super(_Cumulative, self).__init__(
              key='{}({})'.format(self.__class__.__name__.lower(),
                                  getattr(arg, 'key', arg)),
              label='{}({})'.format(self.__class__.__name__,
                                    getattr(arg, 'label', arg)),
              prefix=getattr(arg, 'prefix'),
              suffix=getattr(arg, 'suffix'),
              precision=getattr(arg, 'precision'),
        )

        self.arg = arg

    def apply(self, data_frame, reference):
        raise NotImplementedError()

    @property
    def metrics(self):
        return [metric
                for metric in [self.arg]
                if isinstance(metric, Metric)]

    @property
    def operations(self):
        return [op_and_children
                for operation in [self.arg]
                if isinstance(operation, Operation)
                for op_and_children in [operation] + operation.operations]

    def __repr__(self):
        return self.key


class CumSum(_Cumulative):
    def apply(self, data_frame, reference):
        df_key = format_metric_key(reference_key(self.arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_key] \
                .groupby(level=levels) \
                .cumsum()

        return data_frame[df_key].cumsum()


class CumProd(_Cumulative):
    def apply(self, data_frame, reference):
        df_key = format_metric_key(reference_key(self.arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_key] \
                .groupby(level=levels) \
                .cumprod()

        return data_frame[df_key].cumprod()


class CumMean(_Cumulative):
    @staticmethod
    def cummean(x):
        return x.cumsum() / np.arange(1, len(x) + 1)

    def apply(self, data_frame, reference):
        df_key = format_metric_key(reference_key(self.arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_key] \
                .groupby(level=levels) \
                .apply(self.cummean)

        return self.cummean(data_frame[df_key])


class RollingOperation(_BaseOperation):
    def __init__(self, arg, window, min_periods=None):
        super(RollingOperation, self).__init__(
              key='{}({})'.format(self.__class__.__name__.lower(),
                                  getattr(arg, 'key', arg)),
              label='{}({})'.format(self.__class__.__name__,
                                    getattr(arg, 'label', arg)),
              prefix=getattr(arg, 'prefix'),
              suffix=getattr(arg, 'suffix'),
              precision=getattr(arg, 'precision'),
        )

        self.arg = arg
        self.window = window
        self.min_periods = min_periods

    def _should_adjust(self, other_operations):
        # Need to figure out if this rolling operation is has the largest window, and if it's the first of multiple
        # rolling operations if there are more than one operation sharing the largest window.
        first_max_rolling = list(sorted(other_operations, key=lambda operation: operation.window))[0]

        return first_max_rolling is self

    def apply(self, data_frame, reference):
        raise NotImplementedError()

    @property
    def metrics(self):
        return [metric
                for metric in [self.arg]
                if isinstance(metric, Metric)]

    @property
    def operations(self):
        return [op_and_children
                for operation in [self.arg]
                if isinstance(operation, Operation)
                for op_and_children in [operation] + operation.operations]


class RollingMean(RollingOperation):
    def rolling_mean(self, x):
        return x.rolling(self.window, self.min_periods).mean()

    def apply(self, data_frame, reference):
        df_key = format_metric_key(reference_key(self.arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_key] \
                .groupby(level=levels) \
                .apply(self.rolling_mean)

        return self.rolling_mean(data_frame[df_key])


class Share(_BaseOperation):
    def __init__(self, metric: Metric, over: Dimension = None, precision=2):
        super(Share, self).__init__(
              key='share({},{})'.format(getattr(metric, 'key', metric),
                                        getattr(over, 'key', over), ),
              label='Share of {} over {}'.format(getattr(metric, 'label', metric),
                                                 getattr(over, 'label', over)),
              prefix=None,
              suffix='%',
              precision=precision,
        )

        self.metric = metric
        self.over = over

    @property
    def metrics(self):
        return [metric
                for metric in [self.metric]
                if isinstance(metric, Metric)]

    @property
    def operations(self):
        return [op_and_children
                for operation in [self.metric]
                if isinstance(operation, Operation)
                for op_and_children in [operation] + operation.operations]

    def apply(self, data_frame, reference):
        f_metric_key = format_metric_key(reference_key(self.metric, reference))

        if self.over is None:
            df = data_frame[f_metric_key]
            return 100 * df / df

        if not isinstance(data_frame.index, pd.MultiIndex):
            marker = get_totals_marker_for_dtype(data_frame.index.dtype)
            totals = data_frame.loc[marker, f_metric_key]
            return 100 * data_frame[f_metric_key] / totals

        f_over_key = format_dimension_key(self.over.key)
        idx = data_frame.index.names.index(f_over_key)
        group_levels = data_frame.index.names[idx:]
        over_dim_value = get_totals_marker_for_dtype(data_frame.index.levels[idx].dtype)
        totals_key = (slice(None),) * idx + (slice(over_dim_value, over_dim_value),)

        totals = reduce_data_frame_levels(data_frame.loc[totals_key, f_metric_key], group_levels)

        def apply_totals(df):
            return 100 * reduce_data_frame_levels(df / totals, group_levels)

        return data_frame[f_metric_key] \
            .groupby(level=group_levels) \
            .apply(apply_totals) \
            .reorder_levels(order=data_frame.index.names) \
            .sort_index()
