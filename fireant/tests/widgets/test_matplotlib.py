from unittest import TestCase

from fireant.tests.dataset.mocks import (
    dimx1_date_df,
    mock_dataset,
)
from fireant.widgets.matplotlib import Matplotlib

try:
    # travis-ci cannot run these tests because it won't install matplotlib
    import matplotlib

    class MatplotlibLineChartTransformerTests(TestCase):
        maxDiff = None

        chart_class = Matplotlib.LineSeries
        chart_type = 'line'
        stacking = None

        def test_single_metric_line_chart(self):
            result = (
                Matplotlib(title="Time Series, Single Metric")
                .axis(self.chart_class(mock_dataset.fields.votes))
                .transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])
            )

            self.assertEqual(1, len(result))


except ImportError:
    pass
