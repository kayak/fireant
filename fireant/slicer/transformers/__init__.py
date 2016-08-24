# coding: utf-8

from .base import Transformer, TransformationException
from .datatables import DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer
from .datatables import (DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer, CSVRowIndexTransformer,
                         CSVColumnIndexTransformer)
from .highcharts import HighchartsLineTransformer, HighchartsColumnTransformer, HighchartsBarTransformer
from .notebook import PandasRowIndexTransformer, PandasColumnIndexTransformer

notebook_tx = {
    'row_index_table': PandasRowIndexTransformer(),
    'column_index_table': PandasColumnIndexTransformer(),
}

try:
    from .notebook import MatplotlibLineChartTransformer, MatplotlibBarChartTransformer

    notebook_tx['line_chart'] = MatplotlibLineChartTransformer()
    notebook_tx['bar_chart'] = MatplotlibBarChartTransformer()

except ImportError:
    # Matplotlib not installed
    pass

bundles = {
    'notebook': notebook_tx,
    'highcharts': {
        'line_chart': HighchartsLineTransformer(),
        'column_chart': HighchartsColumnTransformer(),
        'bar_chart': HighchartsBarTransformer(),
    },
    'datatables': {
        'row_index_table': DataTablesRowIndexTransformer(),
        'column_index_table': DataTablesColumnIndexTransformer(),
        'row_index_csv': CSVRowIndexTransformer(),
        'column_index_csv': CSVColumnIndexTransformer(),
    },
}
