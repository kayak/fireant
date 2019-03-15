import numpy as np
import pandas as pd

from fireant.dataset.totals import get_totals_marker_for_dtype
from fireant.utils import (
    alias_selector,
    reduce_data_frame_levels,
)
from .fields import (
    DataType,
    Field,
)
from ..reference_helpers import reference_alias


def _extract_key_or_arg(data_frame, key):
    return data_frame[key] \
        if key in data_frame \
        else key


class Operation:
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
    data_type = DataType.number

    def __init__(self,
                 alias,
                 label,
                 args,
                 prefix: str = None,
                 suffix: str = None,
                 precision: int = None):
        self.alias = alias
        self.label = label
        self.args = args
        self.prefix = prefix
        self.suffix = suffix
        self.precision = precision

    def apply(self, data_frame, reference):
        raise NotImplementedError()

    @property
    def metrics(self):
        return [metric
                for metric in self.args
                if isinstance(metric, Field)]

    @property
    def operations(self):
        return [op_and_children
                for operation in self.args
                if isinstance(operation, Operation)
                for op_and_children in [operation] + operation.operations]

    def _group_levels(self, index):
        """
        Get the index levels that need to be grouped. This is to avoid apply the cumulative function across separate
        dimensions. Only the first dimension should be accumulated across.

        :param index:
        :return:
        """
        return index.names[1:]

    def __repr__(self):
        return self.alias


class _Cumulative(_BaseOperation):
    def __init__(self, arg):
        super(_Cumulative, self).__init__(
              alias='{}({})'.format(self.__class__.__name__.lower(),
                                    getattr(arg, 'alias', arg)),
              label='{}({})'.format(self.__class__.__name__,
                                    getattr(arg, 'label', arg)),
              args=[arg],
              prefix=getattr(arg, 'prefix'),
              suffix=getattr(arg, 'suffix'),
              precision=getattr(arg, 'precision'),
        )

    def apply(self, data_frame, reference):
        raise NotImplementedError()


class CumSum(_Cumulative):
    def apply(self, data_frame, reference):
        arg, = self.args
        df_key = alias_selector(reference_alias(arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_key] \
                .groupby(level=levels) \
                .cumsum()

        return data_frame[df_key].cumsum()


class CumProd(_Cumulative):
    def apply(self, data_frame, reference):
        arg, = self.args
        df_key = alias_selector(reference_alias(arg, reference))

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
        arg = self.args[0]
        df_key = alias_selector(reference_alias(arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_key] \
                .groupby(level=levels) \
                .apply(self.cummean)

        return self.cummean(data_frame[df_key])


class RollingOperation(_BaseOperation):
    def __init__(self, arg, window, min_periods=None):
        super(RollingOperation, self).__init__(
              alias='{}({},{})'.format(self.__class__.__name__.lower(),
                                       getattr(arg, 'alias', arg),
                                       window),
              label='{}({},{})'.format(self.__class__.__name__,
                                       getattr(arg, 'label', arg),
                                       window),
              args=[arg],
              prefix=getattr(arg, 'prefix'),
              suffix=getattr(arg, 'suffix'),
              precision=getattr(arg, 'precision'),
        )
        self.window = window
        self.min_periods = min_periods

    def _should_adjust(self, other_operations):
        # Need to figure out if this rolling operation is has the largest window, and if it's the first of multiple
        # rolling operations if there are more than one operation sharing the largest window.
        first_max_rolling = list(sorted(other_operations, key=lambda operation: operation.window))[0]

        return first_max_rolling is self

    def apply(self, data_frame, reference):
        raise NotImplementedError()


class RollingMean(RollingOperation):
    def rolling_mean(self, x):
        return x.rolling(self.window, self.min_periods).mean()

    def apply(self, data_frame, reference):
        arg, = self.args
        df_alias = alias_selector(reference_alias(arg, reference))

        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[df_alias] \
                .groupby(level=levels) \
                .apply(self.rolling_mean)

        return self.rolling_mean(data_frame[df_alias])


class Share(_BaseOperation):
    def __init__(self, metric: Field, over: Field = None, precision=2):
        super(Share, self).__init__(
              alias='share({},{})'.format(getattr(metric, 'alias', metric),
                                          getattr(over, 'alias', over), ),
              label='Share of {} over {}'.format(getattr(metric, 'label', metric),
                                                 getattr(over, 'label', over)),
              args=[metric, over],
              suffix='%',
              precision=precision,
        )

    @property
    def metric(self):
        return self.args[0]

    @property
    def over(self):
        return self.args[1]

    def apply(self, data_frame, reference):
        metric, over = self.args
        f_metric_alias = alias_selector(reference_alias(metric, reference))

        if over is None:
            df = data_frame[f_metric_alias]
            return 100 * df / df

        if not isinstance(data_frame.index, pd.MultiIndex):
            marker = get_totals_marker_for_dtype(data_frame.index.dtype)
            totals = data_frame.loc[marker, f_metric_alias]
            return 100 * data_frame[f_metric_alias] / totals

        f_over_alias = alias_selector(over.alias)
        idx = data_frame.index.names.index(f_over_alias)
        group_levels = data_frame.index.names[idx:]
        over_dim_value = get_totals_marker_for_dtype(data_frame.index.levels[idx].dtype)
        totals_alias = (slice(None),) * idx + (slice(over_dim_value, over_dim_value),)

        totals = reduce_data_frame_levels(data_frame.loc[totals_alias, f_metric_alias], group_levels)

        def apply_totals(group_df):
            if not isinstance(totals, pd.Series):
                return 100 * group_df / totals

            n_index_levels = len(totals.index.names)
            extra_level_names = group_df.index.names[n_index_levels:]
            group_df = group_df.reset_index(extra_level_names, drop=True)
            share = 100 * group_df / totals[group_df.index]
            return pd.Series(share.values, index=group_df.index)

        return data_frame[f_metric_alias] \
            .groupby(level=group_levels) \
            .apply(apply_totals) \
            .reorder_levels(order=data_frame.index.names) \
            .sort_index()
