# coding: utf-8
from collections import OrderedDict
from datetime import date, datetime
from unittest import TestCase

import numpy as np
import pandas as pd

from fireant import settings
from fireant.slicer.transformers import DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer
from fireant.slicer.transformers import datatables
from fireant.tests.slicer.transformers.base import BaseTransformerTests


class DataTablesRowIndexTransformerTests(BaseTransformerTests):
    dt_tx = DataTablesRowIndexTransformer()

    def test_no_dims_multi_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(self.no_dims_multi_metric_df, self.no_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'},
                        {'data': 'three', 'title': 'Three'},
                        {'data': 'four', 'title': 'Four'},
                        {'data': 'five', 'title': 'Five'},
                        {'data': 'six', 'title': 'Six'},
                        {'data': 'seven', 'title': 'Seven'},
                        {'data': 'eight', 'title': 'Eight'}],
            'data': [{'one': 0, 'two': 1, 'four': 3, 'five': 4, 'six': 5, 'three': 2, 'seven': 6, 'eight': 7}]}, result)

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(self.cont_dim_single_metric_df, self.cont_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'one', 'title': 'One'}],
            'data': [{'cont': {'display': 0}, 'one': 0},
                     {'cont': {'display': 1}, 'one': 1},
                     {'cont': {'display': 2}, 'one': 2},
                     {'cont': {'display': 3}, 'one': 3},
                     {'cont': {'display': 4}, 'one': 4},
                     {'cont': {'display': 5}, 'one': 5},
                     {'cont': {'display': 6}, 'one': 6},
                     {'cont': {'display': 7}, 'one': 7}]}, result)

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        result = self.dt_tx.transform(self.cont_dim_multi_metric_df, self.cont_dim_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'}],
            'data': [{'cont': {'display': 0}, 'one': 0, 'two': 0},
                     {'cont': {'display': 1}, 'one': 1, 'two': 2},
                     {'cont': {'display': 2}, 'one': 2, 'two': 4},
                     {'cont': {'display': 3}, 'one': 3, 'two': 6},
                     {'cont': {'display': 4}, 'one': 4, 'two': 8},
                     {'cont': {'display': 5}, 'one': 5, 'two': 10},
                     {'cont': {'display': 6}, 'one': 6, 'two': 12},
                     {'cont': {'display': 7}, 'one': 7, 'two': 14}]}, result)

    def test_time_series_date(self):
        # Tests transformation of a single-metric, single-dimension result
        result = self.dt_tx.transform(self.time_dim_single_metric_df, self.time_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'date.display', 'title': 'Date'},
                        {'data': 'one', 'title': 'One'}],
            'data': [{'date': {'display': '2000-01-01'}, 'one': 0},
                     {'date': {'display': '2000-01-02'}, 'one': 1},
                     {'date': {'display': '2000-01-03'}, 'one': 2},
                     {'date': {'display': '2000-01-04'}, 'one': 3},
                     {'date': {'display': '2000-01-05'}, 'one': 4},
                     {'date': {'display': '2000-01-06'}, 'one': 5},
                     {'date': {'display': '2000-01-07'}, 'one': 6},
                     {'date': {'display': '2000-01-08'}, 'one': 7}]}, result)

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.dt_tx.transform(self.time_dim_single_metric_ref_df, self.time_dim_single_metric_ref_schema)
        self.assertDictEqual({
            'columns': [{'data': 'date.display', 'title': 'Date'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'wow.one', 'title': 'One WoW'}],
            'data': [{'date': {'display': '2000-01-01'}, 'one': 0, 'wow': {'one': 0}},
                     {'date': {'display': '2000-01-02'}, 'one': 1, 'wow': {'one': 2}},
                     {'date': {'display': '2000-01-03'}, 'one': 2, 'wow': {'one': 4}},
                     {'date': {'display': '2000-01-04'}, 'one': 3, 'wow': {'one': 6}},
                     {'date': {'display': '2000-01-05'}, 'one': 4, 'wow': {'one': 8}},
                     {'date': {'display': '2000-01-06'}, 'one': 5, 'wow': {'one': 10}},
                     {'date': {'display': '2000-01-07'}, 'one': 6, 'wow': {'one': 12}},
                     {'date': {'display': '2000-01-08'}, 'one': 7, 'wow': {'one': 14}}]}, result)

    def test_uni_dim_single_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and display
        result = self.dt_tx.transform(self.uni_dim_single_metric_df, self.uni_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'uni.display', 'title': 'Uni'},
                        {'data': 'one', 'title': 'One'}],
            'data': [{'uni': {'value': 1, 'display': 'Aa'}, 'one': 0},
                     {'uni': {'value': 2, 'display': 'Bb'}, 'one': 1},
                     {'uni': {'value': 3, 'display': 'Cc'}, 'one': 2}]}, result)

    def test_uni_dim_multi_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and display
        result = self.dt_tx.transform(self.uni_dim_multi_metric_df, self.uni_dim_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'uni.display', 'title': 'Uni'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'}],
            'data': [{'uni': {'value': 1, 'display': 'Aa'}, 'one': 0, 'two': 0},
                     {'uni': {'value': 2, 'display': 'Bb'}, 'one': 1, 'two': 2},
                     {'uni': {'value': 3, 'display': 'Cc'}, 'one': 2, 'two': 4}]}, result)

    def test_cat_cat_dim_single_metric(self):
        # Tests transformation of a single metric with two categorical dimensions
        result = self.dt_tx.transform(self.cat_cat_dims_single_metric_df, self.cat_cat_dims_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cat1.display', 'title': 'Cat1'},
                        {'data': 'cat2.display', 'title': 'Cat2'},
                        {'data': 'one', 'title': 'One'}],
            'data': [{'cat1': {'value': 'a', 'display': 'A'},
                      'cat2': {'value': 'y', 'display': 'Y'},
                      'one': 0},
                     {'cat1': {'value': 'a', 'display': 'A'},
                      'cat2': {'value': 'z', 'display': 'Z'},
                      'one': 1},
                     {'cat1': {'value': 'b', 'display': 'B'},
                      'cat2': {'value': 'y', 'display': 'Y'},
                      'one': 2},
                     {'cat1': {'value': 'b', 'display': 'B'},
                      'cat2': {'value': 'z', 'display': 'Z'},
                      'one': 3}]}, result)

    def test_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        result = self.dt_tx.transform(self.cat_cat_dims_multi_metric_df, self.cat_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cat1.display', 'title': 'Cat1'},
                        {'data': 'cat2.display', 'title': 'Cat2'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'}],
            'data': [{'cat1': {'value': 'a', 'display': 'A'},
                      'cat2': {'value': 'y', 'display': 'Y'},
                      'one': 0, 'two': 0},
                     {'cat1': {'value': 'a', 'display': 'A'},
                      'cat2': {'value': 'z', 'display': 'Z'},
                      'one': 1, 'two': 2},
                     {'cat1': {'value': 'b', 'display': 'B'},
                      'cat2': {'value': 'y', 'display': 'Y'},
                      'one': 2, 'two': 4},
                     {'cat1': {'value': 'b', 'display': 'B'},
                      'cat2': {'value': 'z', 'display': 'Z'},
                      'one': 3, 'two': 6}]}, result)

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'cat1.display', 'title': 'Cat1'},
                        {'data': 'cat2.display', 'title': 'Cat2'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'}],
            'data': [{'cont': {'display': 0},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 12, 'two': 24},
                     {'cont': {'display': 0},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 1, 'two': 2},
                     {'cont': {'display': 0},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 0, 'two': 0},
                     {'cont': {'display': 0},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 1, 'two': 2},
                     {'cont': {'display': 0},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 5, 'two': 10},
                     {'cont': {'display': 0},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 2, 'two': 4},
                     {'cont': {'display': 0},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 3, 'two': 6},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 44, 'two': 88},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 9, 'two': 18},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 4, 'two': 8},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 5, 'two': 10},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 13, 'two': 26},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 6, 'two': 12},
                     {'cont': {'display': 1},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 7, 'two': 14},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 76, 'two': 152},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 17, 'two': 34},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 8, 'two': 16},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 9, 'two': 18},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 21, 'two': 42},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 10, 'two': 20},
                     {'cont': {'display': 2},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 11, 'two': 22},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 108, 'two': 216},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 25, 'two': 50},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 12, 'two': 24},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 13, 'two': 26},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 29, 'two': 58},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 14, 'two': 28},
                     {'cont': {'display': 3},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 15, 'two': 30},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 140, 'two': 280},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 33, 'two': 66},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 16, 'two': 32},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 17, 'two': 34},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 37, 'two': 74},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 18, 'two': 36},
                     {'cont': {'display': 4},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 19, 'two': 38},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 172, 'two': 344},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 41, 'two': 82},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 20, 'two': 40},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 21, 'two': 42},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 45, 'two': 90},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 22, 'two': 44},
                     {'cont': {'display': 5},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 23, 'two': 46},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 204, 'two': 408},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 49, 'two': 98},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 24, 'two': 48},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 25, 'two': 50},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 53, 'two': 106},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 26, 'two': 52},
                     {'cont': {'display': 6},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 27, 'two': 54},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'Total', 'value': None},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 236, 'two': 472},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 57, 'two': 114},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 28, 'two': 56},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'A', 'value': 'a'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 29, 'two': 58},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Total', 'value': None},
                      'one': 61, 'two': 122},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Y', 'value': 'y'},
                      'one': 30, 'two': 60},
                     {'cont': {'display': 7},
                      'cat1': {'display': 'B', 'value': 'b'},
                      'cat2': {'display': 'Z', 'value': 'z'},
                      'one': 31, 'two': 62}]}, result)


class DataTablesColumnIndexTransformerTests(BaseTransformerTests):
    dt_tx = DataTablesColumnIndexTransformer()

    def test_no_dims_multi_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(self.no_dims_multi_metric_df, self.no_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'},
                        {'data': 'three', 'title': 'Three'},
                        {'data': 'four', 'title': 'Four'},
                        {'data': 'five', 'title': 'Five'},
                        {'data': 'six', 'title': 'Six'},
                        {'data': 'seven', 'title': 'Seven'},
                        {'data': 'eight', 'title': 'Eight'}],
            'data': [{'one': 0, 'two': 1, 'four': 3, 'five': 4, 'six': 5, 'three': 2, 'seven': 6, 'eight': 7}]}, result)

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(self.cont_dim_single_metric_df, self.cont_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'one', 'title': 'One'}],
            'data': [{'cont': {'display': 0}, 'one': 0},
                     {'cont': {'display': 1}, 'one': 1},
                     {'cont': {'display': 2}, 'one': 2},
                     {'cont': {'display': 3}, 'one': 3},
                     {'cont': {'display': 4}, 'one': 4},
                     {'cont': {'display': 5}, 'one': 5},
                     {'cont': {'display': 6}, 'one': 6},
                     {'cont': {'display': 7}, 'one': 7}]}, result)

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        result = self.dt_tx.transform(self.cont_dim_multi_metric_df, self.cont_dim_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'two', 'title': 'Two'}],
            'data': [{'cont': {'display': 0}, 'one': 0, 'two': 0},
                     {'cont': {'display': 1}, 'one': 1, 'two': 2},
                     {'cont': {'display': 2}, 'one': 2, 'two': 4},
                     {'cont': {'display': 3}, 'one': 3, 'two': 6},
                     {'cont': {'display': 4}, 'one': 4, 'two': 8},
                     {'cont': {'display': 5}, 'one': 5, 'two': 10},
                     {'cont': {'display': 6}, 'one': 6, 'two': 12},
                     {'cont': {'display': 7}, 'one': 7, 'two': 14}]}, result)

    def test_time_series_date_to_millis(self):
        # Tests transformation of a single-metric, single-dimension result
        result = self.dt_tx.transform(self.time_dim_single_metric_df, self.time_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'date.display', 'title': 'Date'},
                        {'data': 'one', 'title': 'One'}],
            'data': [{'date': {'display': '2000-01-01'}, 'one': 0},
                     {'date': {'display': '2000-01-02'}, 'one': 1},
                     {'date': {'display': '2000-01-03'}, 'one': 2},
                     {'date': {'display': '2000-01-04'}, 'one': 3},
                     {'date': {'display': '2000-01-05'}, 'one': 4},
                     {'date': {'display': '2000-01-06'}, 'one': 5},
                     {'date': {'display': '2000-01-07'}, 'one': 6},
                     {'date': {'display': '2000-01-08'}, 'one': 7}]}, result)

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.dt_tx.transform(self.time_dim_single_metric_ref_df, self.time_dim_single_metric_ref_schema)
        self.assertDictEqual({
            'columns': [{'data': 'date.display', 'title': 'Date'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'wow.one', 'title': 'One WoW'}],
            'data': [{'date': {'display': '2000-01-01'}, 'one': 0, 'wow': {'one': 0}},
                     {'date': {'display': '2000-01-02'}, 'one': 1, 'wow': {'one': 2}},
                     {'date': {'display': '2000-01-03'}, 'one': 2, 'wow': {'one': 4}},
                     {'date': {'display': '2000-01-04'}, 'one': 3, 'wow': {'one': 6}},
                     {'date': {'display': '2000-01-05'}, 'one': 4, 'wow': {'one': 8}},
                     {'date': {'display': '2000-01-06'}, 'one': 5, 'wow': {'one': 10}},
                     {'date': {'display': '2000-01-07'}, 'one': 6, 'wow': {'one': 12}},
                     {'date': {'display': '2000-01-08'}, 'one': 7, 'wow': {'one': 14}}]}, result)

    def test_cont_cat_dim_single_metric(self):
        # Tests transformation of a single metric with a continuous and a categorical dimension
        result = self.dt_tx.transform(self.cont_cat_dims_single_metric_df, self.cont_cat_dims_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'a.one', 'title': 'One (A)'},
                        {'data': 'b.one', 'title': 'One (B)'}],
            'data': [{'cont': {'display': 0}, 'a': {'one': 0}, 'b': {'one': 1},},
                     {'cont': {'display': 1}, 'a': {'one': 2}, 'b': {'one': 3},},
                     {'cont': {'display': 2}, 'a': {'one': 4}, 'b': {'one': 5},},
                     {'cont': {'display': 3}, 'a': {'one': 6}, 'b': {'one': 7},},
                     {'cont': {'display': 4}, 'a': {'one': 8}, 'b': {'one': 9},},
                     {'cont': {'display': 5}, 'a': {'one': 10}, 'b': {'one': 11},},
                     {'cont': {'display': 6}, 'a': {'one': 12}, 'b': {'one': 13},},
                     {'cont': {'display': 7}, 'a': {'one': 14}, 'b': {'one': 15},}]}, result)

    def test_cont_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and a categorical dimension
        result = self.dt_tx.transform(self.cont_cat_dims_multi_metric_df, self.cont_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'a.one', 'title': 'One (A)'},
                        {'data': 'b.one', 'title': 'One (B)'},
                        {'data': 'a.two', 'title': 'Two (A)'},
                        {'data': 'b.two', 'title': 'Two (B)'}],
            'data': [{'cont': {'display': 0},
                      'a': {'one': 0, 'two': 0},
                      'b': {'one': 1, 'two': 2}},
                     {'cont': {'display': 1},
                      'a': {'one': 2, 'two': 4},
                      'b': {'one': 3, 'two': 6}},
                     {'cont': {'display': 2},
                      'a': {'one': 4, 'two': 8},
                      'b': {'one': 5, 'two': 10}},
                     {'cont': {'display': 3},
                      'a': {'one': 6, 'two': 12},
                      'b': {'one': 7, 'two': 14}},
                     {'cont': {'display': 4},
                      'a': {'one': 8, 'two': 16},
                      'b': {'one': 9, 'two': 18}},
                     {'cont': {'display': 5},
                      'a': {'one': 10, 'two': 20},
                      'b': {'one': 11, 'two': 22}},
                     {'cont': {'display': 6},
                      'a': {'one': 12, 'two': 24},
                      'b': {'one': 13, 'two': 26}},
                     {'cont': {'display': 7},
                      'a': {'one': 14, 'two': 28},
                      'b': {'one': 15, 'two': 30}}]}, result)

    def test_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(self.cont_cat_cat_dims_multi_metric_df,
                                      self.cont_cat_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'a.y.one', 'title': 'One (A, Y)'},
                        {'data': 'a.z.one', 'title': 'One (A, Z)'},
                        {'data': 'b.y.one', 'title': 'One (B, Y)'},
                        {'data': 'b.z.one', 'title': 'One (B, Z)'},
                        {'data': 'a.y.two', 'title': 'Two (A, Y)'},
                        {'data': 'a.z.two', 'title': 'Two (A, Z)'},
                        {'data': 'b.y.two', 'title': 'Two (B, Y)'},
                        {'data': 'b.z.two', 'title': 'Two (B, Z)'}],
            'data': [{'cont': {'display': 0},
                      'a': {'y': {'one': 0, 'two': 0}, 'z': {'one': 1, 'two': 2}},
                      'b': {'y': {'one': 2, 'two': 4}, 'z': {'one': 3, 'two': 6}}},
                     {'cont': {'display': 1},
                      'a': {'y': {'one': 4, 'two': 8}, 'z': {'one': 5, 'two': 10}},
                      'b': {'y': {'one': 6, 'two': 12}, 'z': {'one': 7, 'two': 14}}},
                     {'cont': {'display': 2},
                      'a': {'y': {'one': 8, 'two': 16}, 'z': {'one': 9, 'two': 18}},
                      'b': {'y': {'one': 10, 'two': 20}, 'z': {'one': 11, 'two': 22}}},
                     {'cont': {'display': 3},
                      'a': {'y': {'one': 12, 'two': 24}, 'z': {'one': 13, 'two': 26}},
                      'b': {'y': {'one': 14, 'two': 28}, 'z': {'one': 15, 'two': 30}}},
                     {'cont': {'display': 4},
                      'a': {'y': {'one': 16, 'two': 32}, 'z': {'one': 17, 'two': 34}},
                      'b': {'y': {'one': 18, 'two': 36}, 'z': {'one': 19, 'two': 38}}},
                     {'cont': {'display': 5},
                      'a': {'y': {'one': 20, 'two': 40}, 'z': {'one': 21, 'two': 42}},
                      'b': {'y': {'one': 22, 'two': 44}, 'z': {'one': 23, 'two': 46}}},
                     {'cont': {'display': 6},
                      'a': {'y': {'one': 24, 'two': 48}, 'z': {'one': 25, 'two': 50}},
                      'b': {'y': {'one': 26, 'two': 52}, 'z': {'one': 27, 'two': 54}}},
                     {'cont': {'display': 7},
                      'a': {'y': {'one': 28, 'two': 56}, 'z': {'one': 29, 'two': 58}},
                      'b': {'y': {'one': 30, 'two': 60}, 'z': {'one': 31, 'two': 62}}}]}, result)

    def test_cont_cat_uni_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(self.cont_cat_uni_dims_multi_metric_df,
                                      self.cont_cat_uni_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'a.1.one', 'title': 'One (A, Aa)'},
                        {'data': 'a.2.one', 'title': 'One (A, Bb)'},
                        {'data': 'a.3.one', 'title': 'One (A, Cc)'},
                        {'data': 'b.1.one', 'title': 'One (B, Aa)'},
                        {'data': 'b.2.one', 'title': 'One (B, Bb)'},
                        {'data': 'b.3.one', 'title': 'One (B, Cc)'},
                        {'data': 'a.1.two', 'title': 'Two (A, Aa)'},
                        {'data': 'a.2.two', 'title': 'Two (A, Bb)'},
                        {'data': 'a.3.two', 'title': 'Two (A, Cc)'},
                        {'data': 'b.1.two', 'title': 'Two (B, Aa)'},
                        {'data': 'b.2.two', 'title': 'Two (B, Bb)'},
                        {'data': 'b.3.two', 'title': 'Two (B, Cc)'}],
            'data': [{'a': {1: {'one': 0, 'two': 100}, 2: {'one': 1, 'two': 101}, 3: {'one': 2, 'two': 102}},
                      'b': {1: {'one': 3, 'two': 103}, 2: {'one': 4, 'two': 104}, 3: {'one': 5, 'two': 105}},
                      'cont': {'display': 0}},
                     {'a': {1: {'one': 6, 'two': 106}, 2: {'one': 7, 'two': 107}, 3: {'one': 8, 'two': 108}},
                      'b': {1: {'one': 9, 'two': 109}, 2: {'one': 10, 'two': 110}, 3: {'one': 11, 'two': 111}},
                      'cont': {'display': 1}},
                     {'a': {1: {'one': 12, 'two': 112}, 2: {'one': 13, 'two': 113}, 3: {'one': 14, 'two': 114}},
                      'b': {1: {'one': 15, 'two': 115}, 2: {'one': 16, 'two': 116}, 3: {'one': 17, 'two': 117}},
                      'cont': {'display': 2}},
                     {'a': {1: {'one': 18, 'two': 118}, 2: {'one': 19, 'two': 119}, 3: {'one': 20, 'two': 120}},
                      'b': {1: {'one': 21, 'two': 121}, 2: {'one': 22, 'two': 122}, 3: {'one': 23, 'two': 123}},
                      'cont': {'display': 3}},
                     {'a': {1: {'one': 24, 'two': 124}, 2: {'one': 25, 'two': 125}, 3: {'one': 26, 'two': 126}},
                      'b': {1: {'one': 27, 'two': 127}, 2: {'one': 28, 'two': 128}, 3: {'one': 29, 'two': 129}},
                      'cont': {'display': 4}},
                     {'a': {1: {'one': 30, 'two': 130}, 2: {'one': 31, 'two': 131}, 3: {'one': 32, 'two': 132}},
                      'b': {1: {'one': 33, 'two': 133}, 2: {'one': 34, 'two': 134}, 3: {'one': 35, 'two': 135}},
                      'cont': {'display': 5}},
                     {'a': {1: {'one': 36, 'two': 136}, 2: {'one': 37, 'two': 137}, 3: {'one': 38, 'two': 138}},
                      'b': {1: {'one': 39, 'two': 139}, 2: {'one': 40, 'two': 140}, 3: {'one': 41, 'two': 141}},
                      'cont': {'display': 6}},
                     {'a': {1: {'one': 42, 'two': 142}, 2: {'one': 43, 'two': 143}, 3: {'one': 44, 'two': 144}},
                      'b': {1: {'one': 45, 'two': 145}, 2: {'one': 46, 'two': 146}, 3: {'one': 47, 'two': 147}},
                      'cont': {'display': 7}}]}, result)

    def test_rollup_cont_cat_cat_dims_multi_metric_df(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(self.rollup_cont_cat_cat_dims_multi_metric_df,
                                      self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.assertDictEqual({
            'columns': [{'data': 'cont.display', 'title': 'Cont'},
                        {'data': 'one', 'title': 'One'},
                        {'data': 'a.one', 'title': 'One (A)'},
                        {'data': 'a.y.one', 'title': 'One (A, Y)'},
                        {'data': 'a.z.one', 'title': 'One (A, Z)'},
                        {'data': 'b.one', 'title': 'One (B)'},
                        {'data': 'b.y.one', 'title': 'One (B, Y)'},
                        {'data': 'b.z.one', 'title': 'One (B, Z)'},
                        {'data': 'two', 'title': 'Two'},
                        {'data': 'a.two', 'title': 'Two (A)'},
                        {'data': 'a.y.two', 'title': 'Two (A, Y)'},
                        {'data': 'a.z.two', 'title': 'Two (A, Z)'},
                        {'data': 'b.two', 'title': 'Two (B)'},
                        {'data': 'b.y.two', 'title': 'Two (B, Y)'},
                        {'data': 'b.z.two', 'title': 'Two (B, Z)'}],
            'data': [{'a': {'y': {'one': 0, 'two': 0}, 'z': {'one': 1, 'two': 2}},
                      'b': {'y': {'one': 2, 'two': 4}, 'z': {'one': 3, 'two': 6}},
                      'cont': {'display': 0}},
                     {'a': {'y': {'one': 4, 'two': 8}, 'z': {'one': 5, 'two': 10}},
                      'b': {'y': {'one': 6, 'two': 12}, 'z': {'one': 7, 'two': 14}},
                      'cont': {'display': 1}},
                     {'a': {'y': {'one': 8, 'two': 16}, 'z': {'one': 9, 'two': 18}},
                      'b': {'y': {'one': 10, 'two': 20}, 'z': {'one': 11, 'two': 22}},
                      'cont': {'display': 2}},
                     {'a': {'y': {'one': 12, 'two': 24}, 'z': {'one': 13, 'two': 26}},
                      'b': {'y': {'one': 14, 'two': 28}, 'z': {'one': 15, 'two': 30}},
                      'cont': {'display': 3}},
                     {'a': {'y': {'one': 16, 'two': 32}, 'z': {'one': 17, 'two': 34}},
                      'b': {'y': {'one': 18, 'two': 36}, 'z': {'one': 19, 'two': 38}},
                      'cont': {'display': 4}},
                     {'a': {'y': {'one': 20, 'two': 40}, 'z': {'one': 21, 'two': 42}},
                      'b': {'y': {'one': 22, 'two': 44}, 'z': {'one': 23, 'two': 46}},
                      'cont': {'display': 5}},
                     {'a': {'y': {'one': 24, 'two': 48}, 'z': {'one': 25, 'two': 50}},
                      'b': {'y': {'one': 26, 'two': 52}, 'z': {'one': 27, 'two': 54}},
                      'cont': {'display': 6}},
                     {'a': {'y': {'one': 28, 'two': 56}, 'z': {'one': 29, 'two': 58}},
                      'b': {'y': {'one': 30, 'two': 60}, 'z': {'one': 31, 'two': 62}},
                      'cont': {'display': 7}}]}, result)

    def test_max_cols(self, ):
        settings.datatables_maxcols = 24

        df = self.cont_cat_cat_dims_multi_metric_df.reorder_levels([1, 2, 0])
        schema = self.cont_cat_cat_dims_multi_metric_schema.copy()
        schema['dimensions'] = OrderedDict([(k, schema['dimensions'][k])
                                            for k in ['cat1', 'cat2', 'cont']])

        result = self.dt_tx.transform(df, schema)

        self.assertEqual(24, len(result['columns']))

        for column in result['columns']:
            data_location = column['data']
            levels = data_location.split('.')

            data = result['data'][0]
            for i, level in enumerate(levels):
                self.assertIn(level, data, msg='Missing data level %d [%s] in %s.' % (i, level, data_location))
                data = data[level]


class DatatablesUtilityTests(TestCase):
    def test_nan_data_point(self):
        # Needs to be cast to python int
        result = datatables._format_data_point(np.nan)
        self.assertIsNone(result)

    def test_str_data_point(self):
        result = datatables._format_data_point(u'abc')
        self.assertEqual('abc', result)

    def test_int64_data_point(self):
        # Needs to be cast to python int
        result = datatables._format_data_point(np.int64(1))
        self.assertEqual(int(1), result)

    def test_date_data_point(self):
        # Needs to be converted to milliseconds
        result = datatables._format_data_point(pd.Timestamp(date(2000, 1, 1)))
        self.assertEqual('2000-01-01', result)

    def test_datetime_data_point(self):
        # Needs to be converted to milliseconds
        result = datatables._format_data_point(pd.Timestamp(datetime(2000, 1, 1, 1)))
        self.assertEqual('2000-01-01T01:00:00', result)
