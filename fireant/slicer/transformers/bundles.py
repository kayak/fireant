# coding: utf-8
from . import (HighchartsLineTransformer, HighchartsColumnTransformer, HighchartsBarTransformer,
               DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer, PandasTransformer)

notebook_tx = {
    'row_index': PandasTransformer(),
    'column_index': PandasTransformer(),
}

try:
    from fireant.slicer.transformers.notebook import MatplotlibTransformer

    notebook_tx['line'] = MatplotlibTransformer()
    notebook_tx['column'] = MatplotlibTransformer()
    notebook_tx['bar'] = MatplotlibTransformer()

except ImportError:
    # Matplotlib not installed
    pass

bundles = {
    'notebook': notebook_tx,
    'highcharts': {
        'line': HighchartsLineTransformer(),
        'column ': HighchartsColumnTransformer(),
        'bar': HighchartsBarTransformer(),
    },
    'datatables': {
        'row_index': DataTablesRowIndexTransformer(),
        'column_index': DataTablesColumnIndexTransformer(),
    },
}
