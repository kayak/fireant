from fireant.slicer.exceptions import MetricRequiredException
from fireant.utils import immutable
from ..operations import Operation


class Widget:
    def __init__(self, items=()):
        self.items = list(items)

    @immutable
    def item(self, item):
        self.items.append(item)

    @property
    def metrics(self):
        if 0 == len(self.items):
            raise MetricRequiredException(str(self))

        return [metric
                for group in self.items
                for metric in getattr(group, 'metrics', [group])]

    @property
    def operations(self):
        return [item
                for item in self.items
                if isinstance(item, Operation)]

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               ','.join(str(m) for m in self.items))


class TransformableWidget(Widget):
    def transform(self, data_frame, slicer, dimensions):
        raise NotImplementedError()
