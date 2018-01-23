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
        return [metric
                for group in self._metrics
                for metric in getattr(group, 'metrics', [group])]

    def transform(self, data_frame, slicer, dimensions):
        super(MetricsWidget, self).transform(data_frame, slicer, dimensions)
