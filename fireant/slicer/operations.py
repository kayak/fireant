from .metrics import Metric


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` API.
    """
    pass


class _Loss(Operation):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual

    @property
    def metrics(self):
        return [metric
                for metric in [self.expected, self.actual]
                if isinstance(metric, Metric)]


class L1Loss(_Loss): pass


class L2Loss(_Loss): pass


class _Cumulative(Operation):
    def __init__(self, arg):
        self.arg = arg

    @property
    def metrics(self):
        return [metric
                for metric in [self.arg]
                if isinstance(metric, Metric)]


class CumSum(_Cumulative): pass


class CumAvg(_Cumulative): pass
