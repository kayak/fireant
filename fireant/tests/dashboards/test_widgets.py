# coding: utf-8
from unittest import TestCase

from fireant.dashboards import *
from fireant.slicer.transformers import (HighchartsLineTransformer, HighchartsColumnTransformer,
                                         HighchartsBarTransformer,
                                         DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer)


class DashboardTests(TestCase):
    def _widget_test(self, widget_type, metrics, transformer_type):
        widget = widget_type(metrics=metrics)

        self.assertListEqual(metrics, widget.metrics)
        self.assertEqual(type(transformer_type), type(widget.transformer))
        return widget

    def test_line_chart(self):
        self._widget_test(LineChartWidget, ['clicks', 'conversions'], HighchartsLineTransformer())

    def test_bar_chart(self):
        tx = HighchartsBarTransformer()
        widget = self._widget_test(BarChartWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(HighchartsBarTransformer.chart_type, widget.transformer.chart_type)

    def test_column_chart(self):
        tx = HighchartsColumnTransformer()
        widget = self._widget_test(ColumnChartWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(HighchartsColumnTransformer.chart_type, widget.transformer.chart_type)

    def test_row_index_table(self):
        tx = DataTablesRowIndexTransformer()
        widget = self._widget_test(RowIndexTableWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(DataTablesRowIndexTransformer.table_type, widget.transformer.table_type)

    def test_column_index_table(self):
        tx = DataTablesColumnIndexTransformer()
        widget = self._widget_test(ColumnIndexTableWidget, ['clicks', 'conversions'], tx)

        self.assertEqual(DataTablesColumnIndexTransformer.table_type, widget.transformer.table_type)
