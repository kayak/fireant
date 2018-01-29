from fireant.slicer.exceptions import MetricRequiredException
from fireant.utils import immutable


class Widget:
    def transform(self, data_frame, slicer, dimensions):
        raise NotImplementedError()


class MetricsWidget(Widget):
    def __init__(self, metrics=()):
        self._metrics = list(metrics)

    @immutable
    def metric(self, metric):
        self._metrics.append(metric)

    @property
    def metrics(self):
        if 0 == len(self._metrics):
            raise MetricRequiredException(str(self))

        return [metric
                for group in self._metrics
                for metric in getattr(group, 'metrics', [group])]

    def transform(self, data_frame, slicer, dimensions):
        super(MetricsWidget, self).transform(data_frame, slicer, dimensions)

    def __repr__(self):
        return '{}(metrics={})'.format(self.__class__.__name__,
                                       ','.join(str(m) for m in self._metrics))
