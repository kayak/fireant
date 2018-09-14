from typing import (
    Iterable,
    Union,
)

from fireant import (
    Metric,
    Operation,
    utils,
)
from ..exceptions import MetricRequiredException


class Series:
    type = None
    needs_marker = False
    stacking = None

    def __init__(self, metric: Union[Metric, Operation], stacking=None):
        self.metric = metric
        self.stacking = self.stacking or stacking

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__,
                               repr(self.metric))


class ContinuousAxisSeries(Series):
    pass


class Axis:
    def __init__(self, series: Iterable[Series], label=None, y_axis_visible=True):
        self._series = series or []
        self.label = label
        self.y_axis_visible = y_axis_visible

    def __iter__(self):
        return iter(self._series)

    def __len__(self):
        return len(self._series)

    def __repr__(self):
        return "axis({})".format(", ".join(map(repr, self)))


class ChartWidget:
    class LineSeries(ContinuousAxisSeries):
        type = 'line'

    class AreaSeries(ContinuousAxisSeries):
        type = 'area'

    class AreaStackedSeries(AreaSeries):
        stacking = "normal"

    class AreaPercentageSeries(AreaSeries):
        stacking = "percent"

    class PieSeries(Series):
        type = 'pie'

    class BarSeries(Series):
        type = 'bar'

    class StackedBarSeries(BarSeries):
        stacking = "normal"

    class ColumnSeries(Series):
        type = 'column'

    class StackedColumnSeries(ColumnSeries):
        stacking = "normal"

    @utils.immutable
    def axis(self, *series: Series, **kwargs):
        """
        (Immutable) Adds an axis to the Chart.

        :param axis:
        :return:
        """

        self.items.append(Axis(series, **kwargs))

    @property
    def metrics(self):
        """
        :return:
            A set of metrics used in this chart. This collects all metrics across all axes.
        """
        if 0 == len(self.items):
            raise MetricRequiredException(str(self))

        seen = set()
        return [metric
                for axis in self.items
                for series in axis
                for metric in getattr(series.metric, 'metrics', [series.metric])
                if not (metric.key in seen or seen.add(metric.key))]

    @property
    def operations(self):
        return utils.ordered_distinct_list_by_attr([operation
                                                    for axis in self.items
                                                    for series in axis
                                                    if isinstance(series.metric, Operation)
                                                    for operation in [series.metric] + series.metric.operations])
