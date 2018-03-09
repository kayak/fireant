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


class _Cumulative(Operation):
    def __init__(self, arg):
        self.arg = arg
        self.key = '{}({})'.format(self.__class__.__name__.lower(),
                                   getattr(arg, 'key', arg))
        self.label = '{}({})'.format(self.__class__.__name__,
                                     getattr(arg, 'label', arg))
        self.prefix = getattr(arg, 'prefix')
        self.suffix = getattr(arg, 'suffix')
        self.precision = getattr(arg, 'precision')

    def _group_levels(self, index):
        """
        Get the index levels that need to be grouped. This is to avoid apply the cumulative function across separate
        dimensions. Only the first dimension should be accumulated across.

        :param index:
        :return:
        """
        return index.names[1:]

    @property
    def metrics(self):
        return [metric
                for metric in [self.arg]
                if isinstance(metric, Metric)]

    def apply(self, data_frame):
        raise NotImplementedError()

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
