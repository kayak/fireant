from fireant.slicer.exceptions import MetricRequiredException
from fireant.utils import immutable


class Widget:
    def __init__(self, items=()):
        self.items = list(items)

    @immutable
    def metric(self, metric):
        self.items.append(metric)

    @property
    def metrics(self):
        if 0 == len(self.items):
            raise MetricRequiredException(str(self))

        return [metric
                for group in self.items
                for metric in getattr(group, 'metrics', [group])]

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               ','.join(str(m) for m in self.items))


class TransformableWidget(Widget):
    def transform(self, data_frame, slicer, dimensions):
        raise NotImplementedError()
