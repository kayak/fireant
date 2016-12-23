# coding: utf8


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` API.
    """
    key = None
    label = None

    def schemas(self):
        pass

    def metrics(self):
        return []


class Totals(Operation):
    """
    `Operation` for rolling up totals across dimensions in queries.  This will append the totals across a dimension to
    a dimension.
    """
    key = '_total'
    label = 'Total'

    def __init__(self, *dimension_keys):
        self.dimension_keys = dimension_keys


class L1Loss(Operation):
    """
    Performs L1 Loss (mean abs. error) operation on a metric using another metric as the target.
    """
    key = 'l1loss'
    label = 'L1 loss'

    def __init__(self, metric_key, target_metric_key):
        self.metric_key = metric_key
        self.target_metric_key = target_metric_key

    def schemas(self):
        return {
            'key': self.key,
            'metric': self.metric_key,
            'target': self.target_metric_key,
        }

    def metrics(self):
        return [self.metric_key, self.target_metric_key]


class L2Loss(L1Loss):
    """
    Performs L2 Loss (mean sqr. error) operation on a metric using another metric as the target.
    """
    key = 'l2loss'
    label = 'L2 loss'


class CumSum(Operation):
    """
    Accumulates the sum of one or more metrics.
    """
    key = 'cumsum'
    label = 'cum. sum'

    def __init__(self, metric_key):
        self.metric_key = metric_key

    def schemas(self):
        return {
            'key': self.key,
            'metric': self.metric_key,
        }

    def metrics(self):
        return (self.metric_key,)


class CumMean(CumSum):
    """
    Accumulates the mean of one or more metrics
    """
    key = 'cummean'
    label = 'cum. mean'
