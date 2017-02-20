# coding: utf-8

from .base import (Transformer,
                   TransformationException)
from .datatables import (DataTablesRowIndexTransformer,
                         DataTablesColumnIndexTransformer)
from .datatables import (DataTablesRowIndexTransformer,
                         DataTablesColumnIndexTransformer,
                         CSVRowIndexTransformer,
                         CSVColumnIndexTransformer)
from .highcharts import (HighchartsLineTransformer,
                         HighchartsAreaTransformer,
                         HighchartsAreaPercentageTransformer,
                         HighchartsColumnTransformer,
                         HighchartsStackedColumnTransformer,
                         HighchartsBarTransformer,
                         HighchartsStackedBarTransformer,
                         HighchartsPieTransformer)
from .notebooks import (PandasRowIndexTransformer,
                        PandasColumnIndexTransformer,
                        MatplotlibLineChartTransformer,
                        MatplotlibBarChartTransformer)

BUNDLES = {
    'notebooks': {
        'row_index_table': PandasRowIndexTransformer(),
        'column_index_table': PandasColumnIndexTransformer(),
        'line_chart': MatplotlibLineChartTransformer(),
        'bar_chart': MatplotlibBarChartTransformer(),
    },
    'highcharts': {
        'line_chart': HighchartsLineTransformer(),
        'area_chart': HighchartsAreaTransformer(),
        'area_percentage_chart': HighchartsAreaPercentageTransformer(),
        'column_chart': HighchartsColumnTransformer(),
        'stacked_column_chart': HighchartsStackedColumnTransformer(),
        'bar_chart': HighchartsBarTransformer(),
        'stacked_bar_chart': HighchartsStackedBarTransformer(),
        'pie_chart': HighchartsPieTransformer(),
    },
    'datatables': {
        'row_index_table': DataTablesRowIndexTransformer(),
        'column_index_table': DataTablesColumnIndexTransformer(),
        'row_index_csv': CSVRowIndexTransformer(),
        'column_index_csv': CSVColumnIndexTransformer(),
    },
}
