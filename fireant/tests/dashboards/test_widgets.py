# coding: utf-8
from unittest import TestCase

from fireant.dashboards import *
from fireant.slicer.transformers import (HighchartsTransformer, HighchartsColumnTransformer, TableIndex,
                                         DataTablesTransformer)


class DashboardTests(TestCase):
    def _widget_test(self, widget_type, metrics, transformer_type):
        widget = widget_type(metrics=metrics)

        self.assertListEqual(metrics, widget.metrics)
        self.assertEqual(type(transformer_type), type(widget.transformer))
        return widget

    def test_line_chart(self):
        self._widget_test(LineChartWidget, ['clicks', 'conversions'], HighchartsTransformer())

    def test_bar_chart(self):
        tx = HighchartsColumnTransformer(HighchartsColumnTransformer.bar)
        widget = self._widget_test(BarChartWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(HighchartsColumnTransformer.bar, widget.transformer.chart_type)

    def test_column_chart(self):
        tx = HighchartsColumnTransformer(HighchartsColumnTransformer.column)
        widget = self._widget_test(ColumnChartWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(HighchartsColumnTransformer.column, widget.transformer.chart_type)

    def test_row_index_table(self):
        tx = DataTablesTransformer(TableIndex.row_index)
        widget = self._widget_test(RowIndexTableWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(TableIndex.row_index, widget.transformer.table_type)

    def test_column_index_table(self):
        tx = DataTablesTransformer(TableIndex.column_index)
        widget = self._widget_test(ColumnIndexTableWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(TableIndex.column_index, widget.transformer.table_type)
