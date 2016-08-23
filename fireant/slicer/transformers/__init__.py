# coding: utf-8

from .base import Transformer, TransformationException
from .datatables import (DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer, CSVRowIndexTransformer,
                         CSVColumnIndexTransformer)
from .datatables import DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer
from .highcharts import HighchartsLineTransformer, HighchartsColumnTransformer, HighchartsBarTransformer
from .notebook import PlotlyTransformer, PandasTransformer

notebook_tx = {
    'row_index_table': PandasTransformer(),
    'column_index_table': PandasTransformer(),
}

try:
    from fireant.slicer.transformers.notebook import PlotlyTransformer

    notebook_tx['line_chart'] = PlotlyTransformer()
    notebook_tx['column_chart'] = PlotlyTransformer()
    notebook_tx['bar_chart'] = PlotlyTransformer()

except ImportError:
    # Matplotlib not installed
    pass

bundles = {
    # Unfinished
    # 'notebook': notebook_tx,
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
