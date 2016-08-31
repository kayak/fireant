# coding: utf8


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` object.
    """
    pass


class Totals(Operation):
    """
    Operation for rolling up totals across dimensions in queries.
    """
    key = 'totals'

    def __init__(self, *dimension_keys):
        self.dimension_keys = dimension_keys


class L1Loss(Operation):
    """
    Converts the result of one or more metrics into an l1
    """
    key = 'l1_loss'

    def __init__(self, metric_key, error_key):
        self.metric_key = metric_key
        self.error_key = error_key


class L2Loss(L1Loss):
    key = 'l2_loss'
