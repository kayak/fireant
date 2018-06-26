import numpy as np
import pandas as pd

from .metrics import Metric


def _extract_key_or_arg(data_frame, key):
    return data_frame[key] \
        if key in data_frame \
        else key


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` API.
    """

    def apply(self, data_frame):
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
    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[self.arg.key] \
                .groupby(level=levels) \
                .cumsum()

        return data_frame[self.arg.key].cumsum()


class CumProd(_Cumulative):
    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[self.arg.key] \
                .groupby(level=levels) \
                .cumprod()

        return data_frame[self.arg.key].cumprod()


class CumMean(_Cumulative):
    @staticmethod
    def cummean(x):
        return x.cumsum() / np.arange(1, len(x) + 1)

    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[self.arg.key] \
                .groupby(level=levels) \
                .apply(self.cummean)

        return self.cummean(data_frame[self.arg.key])


class _Rolling(_BaseOperation):
    def apply(self, data_frame):
        raise NotImplementedError()

    def __init__(self, arg, window, min_periods=None):
        super(_Rolling, self).__init__(
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


class RollingMean(_Rolling):
    def rolling_mean(self, x):
        return x.rolling(self.window, self.min_periods).mean()

    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            levels = self._group_levels(data_frame.index)

            return data_frame[self.arg.key] \
                .groupby(level=levels) \
                .apply(self.rolling_mean)

        return self.rolling_mean(data_frame[self.arg.key])
