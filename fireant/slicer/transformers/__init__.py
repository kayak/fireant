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

ROW_INDEX_TABLE = 'row_index_table'
ROW_INDEX_CSV = 'row_index_csv'
COLUMN_INDEX_TABLE = 'column_index_table'
COLUMN_INDEX_CSV = 'column_index_csv'
LINE_CHART = 'line_chart'
BAR_CHART = 'bar_chart'
AREA_CHART = 'area_chart'
AREA_PERCENTAGE_CHART = 'area_percentage_chart'
COLUMN_CHART = 'column_chart'
STACKED_COLUMN_CHART = 'stacked_column_chart'
STACKED_BAR_CHART = 'stacked_bar_chart'
PIE_CHART = 'pie_chart'

BUNDLES = {
    'notebooks': {
        ROW_INDEX_TABLE: PandasRowIndexTransformer(),
        COLUMN_INDEX_TABLE: PandasColumnIndexTransformer(),
        LINE_CHART: MatplotlibLineChartTransformer(),
        BAR_CHART: MatplotlibBarChartTransformer(),
    },
    'highcharts': {
        LINE_CHART: HighchartsLineTransformer(),
        AREA_CHART: HighchartsAreaTransformer(),
        AREA_PERCENTAGE_CHART: HighchartsAreaPercentageTransformer(),
        COLUMN_CHART: HighchartsColumnTransformer(),
        STACKED_COLUMN_CHART: HighchartsStackedColumnTransformer(),
        BAR_CHART: HighchartsBarTransformer(),
        STACKED_BAR_CHART: HighchartsStackedBarTransformer(),
        PIE_CHART: HighchartsPieTransformer(),
    },
    'datatables': {
        ROW_INDEX_TABLE: DataTablesRowIndexTransformer(),
        COLUMN_INDEX_TABLE: DataTablesColumnIndexTransformer(),
        ROW_INDEX_CSV: CSVRowIndexTransformer(),
        COLUMN_INDEX_CSV: CSVColumnIndexTransformer(),
    },
}
