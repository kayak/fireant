from unittest import TestCase

from fireant.slicer.widgets.matplotlib import Matplotlib
from fireant.tests.slicer.mocks import (
    cont_dim_df,
    slicer,
)

try:
    # travis-ci cannot run these tests because it won't install matplotlib
    import matplotlib


    class MatplotlibLineChartTransformerTests(TestCase):
        maxDiff = None

        chart_class = Matplotlib.LineSeries
        chart_type = 'line'
        stacking = None

        def test_single_metric_line_chart(self):
            result = Matplotlib(title="Time Series, Single Metric") \
                .axis(self.chart_class(slicer.metrics.votes)) \
                .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

            self.assertEqual(1, len(result))

except ImportError:
    pass
