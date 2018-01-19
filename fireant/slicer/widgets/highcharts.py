from fireant.utils import immutable
from .base import Widget


class HighCharts(Widget):
    class PieChart(Widget):
        pass

    class LineChart(Widget):
        pass

    class ColumnChart(Widget):
        pass

    class BarChart(Widget):
        pass

    def __init__(self, axes=()):
        self.axes = list(axes)

    def metric(self, metric):
        raise NotImplementedError()

    @immutable
    def axis(self, axis):
        self.axes.append(axis)

    @property
    def metrics(self):
        seen = set()
        return [metric
                for axis in self.axes
                for metric in axis.metrics
                if not (metric.key in seen or seen.add(metric.key))]
