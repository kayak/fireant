from fireant.utils import immutable


class Widget(object):
    def __init__(self, metrics=()):
        self._metrics = list(metrics)

    @immutable
    def metric(self, metric):
        self._metrics.append(metric)

    @property
    def metrics(self):
        return [metric
                for group in self._metrics
                for metric in (group.metrics if hasattr(group, 'metrics') else [group])]

    def transform(self, data_frame, slicer):
        raise NotImplementedError()
