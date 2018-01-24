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

    @property
    def metrics(self):
        return [metric
                for metric in [self.arg]
                if isinstance(metric, Metric)]

    def apply(self, data_frame):
        raise NotImplementedError()


class CumSum(_Cumulative):
    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            return data_frame[self.arg.key] \
                .groupby(level=data_frame.index.names[1:]) \
                .cumsum()

        return data_frame[self.arg.key].cumsum()


class CumProd(_Cumulative):
    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            return data_frame[self.arg.key] \
                .groupby(level=data_frame.index.names[1:]) \
                .cumprod()

        return data_frame[self.arg.key].cumprod()


class CumMean(_Cumulative):
    @staticmethod
    def cummean(x):
        return x.cumsum() / np.arange(1, len(x) + 1)

    def apply(self, data_frame):
        if isinstance(data_frame.index, pd.MultiIndex):
            return data_frame[self.arg.key] \
                .groupby(level=data_frame.index.names[1:]) \
                .apply(self.cummean)

        return self.cummean(data_frame[self.arg.key])
