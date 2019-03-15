import itertools

from fireant import utils
from .base import TransformableWidget
from .chart_base import ChartWidget
from ..reference_helpers import (
    reference_alias,
    reference_label,
)

MAP_SERIES_TO_PLOT_FUNC = {
    ChartWidget.LineSeries: 'line',
    ChartWidget.AreaSeries: 'area',
    ChartWidget.AreaStackedSeries: 'area',
    ChartWidget.AreaPercentageSeries: 'area',
    ChartWidget.PieSeries: 'pie',
    ChartWidget.BarSeries: 'bar',
    ChartWidget.StackedBarSeries: 'bar',
    ChartWidget.ColumnSeries: 'bar',
    ChartWidget.StackedColumnSeries: 'bar',
}


class Matplotlib(ChartWidget, TransformableWidget):
    def __init__(self, title=None):
        super(Matplotlib, self).__init__()
        self.title = title

    def transform(self, data_frame, slicer, dimensions, references):
        import matplotlib.pyplot as plt
        data_frame = data_frame.copy()

        n_axes = len(self.items)
        figsize = (14, 5 * n_axes)
        fig, plt_axes = plt.subplots(n_axes,
                                     sharex='row',
                                     figsize=figsize)
        fig.suptitle(self.title)

        if not hasattr(plt_axes, '__iter__'):
            plt_axes = (plt_axes,)

        colors = itertools.cycle('bgrcmyk')
        for axis, plt_axis in zip(self.items, plt_axes):
            for series in axis:
                series_color = next(colors)

                linestyles = itertools.cycle(['-', '--', '-.', ':'])
                for reference in [None] + references:
                    metric = series.metric
                    f_metric_key = utils.alias_selector(reference_alias(metric, reference))
                    f_metric_label = reference_label(metric, reference)

                    plot = self.get_plot_func_for_series_type(data_frame[f_metric_key], f_metric_label, series)
                    plot(ax=plt_axis,
                         title=axis.label,
                         color=series_color,
                         stacked=series.stacking is not None,
                         linestyle=next(linestyles)) \
                        .legend(loc='center left',
                                bbox_to_anchor=(1, 0.5))

        return plt_axes

    @staticmethod
    def get_plot_func_for_series_type(pd_series, label, chart_series):
        pd_series.name = label
        plot = pd_series.plot
        plot_func_name = MAP_SERIES_TO_PLOT_FUNC[type(chart_series)]
        plot_func = getattr(plot, plot_func_name)
        return plot_func
