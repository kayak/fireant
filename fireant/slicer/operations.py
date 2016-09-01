# coding: utf8


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` API.
    """

    def schemas(self):
        pass


class Totals(Operation):
    """
    `Operation` for rolling up totals across dimensions in queries.  This will append the totals across a dimension to
    a dimension.
    """
    key = 'totals'

    def __init__(self, *dimension_keys):
        self.dimension_keys = dimension_keys


class L1Loss(Operation):
    """
    Performs L1 Loss (mean abs. error) operation on a metric using another metric as the target.
    """
    key = 'l1_loss'

    def __init__(self, metric_key, target_metric_key):
        self.metric_key = metric_key
        self.target_metric_key = target_metric_key

    def schemas(self):
        return {
            'key': self.key,
            'metric': self.metric_key,
            'target': self.target_metric_key,
        }


class L2Loss(L1Loss):
    """
    Performs L1 Loss (mean sqr. error) operation on a metric using another metric as the target.
    """
    key = 'l2_loss'


class CumSum(Operation):
    """
    Accumulates the sum of one or more metrics.
    """
    key = 'cumsum'

    def __init__(self, *metric_keys):
        self.metric_keys = metric_keys

    def schemas(self):
        return {
            'key': self.key,
            'metrics': self.metric_keys,
        }


class CumMean(CumSum):
    """
    Accumulates the mean of one or more metrics
    """
    key = 'cummean'
