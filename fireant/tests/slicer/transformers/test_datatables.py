# coding: utf-8
import locale as lc
from collections import OrderedDict
from datetime import date, datetime
from unittest import TestCase

import numpy as np
import pandas as pd
from fireant import settings
from fireant.slicer.operations import Totals
from fireant.slicer.transformers import DataTablesRowIndexTransformer, DataTablesColumnIndexTransformer
from fireant.slicer.transformers import datatables
from fireant.tests import mock_dataframes as mock_df

lc.setlocale(lc.LC_ALL, 'C')


class DataTablesRowIndexTransformerTests(TestCase):
    maxDiff = None
    dt_tx = DataTablesRowIndexTransformer()

    def test_no_dims_multi_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(mock_df.no_dims_multi_metric_df, mock_df.no_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'title': 'One', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'one'},
                        {'title': 'Two', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'two'},
                        {'title': 'Three', 'render': {'type': 'value', '_': 'display', 'sort': 'value'},
                         'data': 'three'},
                        {'title': 'Four', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'four'},
                        {'title': 'Five', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'five'},
                        {'title': 'Six', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'six'},
                        {'title': 'Seven', 'render': {'type': 'value', '_': 'display', 'sort': 'value'},
                         'data': 'seven'},
                        {'title': 'Eight', 'render': {'type': 'value', '_': 'display', 'sort': 'value'},
                         'data': 'eight'}],
            'data': [{'six': {'value': 5, 'display': '5'}, 'seven': {'value': 6, 'display': '6'},
                      'three': {'value': 2, 'display': '2'}, 'one': {'value': 0, 'display': '0'},
                      'two': {'value': 1, 'display': '1'}, 'four': {'value': 3, 'display': '3'},
                      'eight': {'value': 7, 'display': '7'}, 'five': {'value': 4, 'display': '4'}}]}, result)

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(mock_df.cont_dim_single_metric_df, mock_df.cont_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'title': 'Cont'},
                        {'data': 'one', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'One'}],
            'data': [{'cont': {'value': i}, 'one': {'value': i, 'display': str(i)}}
                     for i in range(8)]}, result)

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        result = self.dt_tx.transform(mock_df.cont_dim_multi_metric_df, mock_df.cont_dim_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'title': 'Cont', 'data': 'cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}},
                        {'title': 'One', 'data': 'one', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'title': 'Two', 'data': 'two', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}}],
            'data': [{
                         'cont': {'value': i},
                         'one': {'value': i, 'display': str(i)},
                         'two': {'value': 2 * i, 'display': str(2 * i)}
                     } for i in range(8)]}, result)

    def test_time_series_date(self):
        # Tests transformation of a single-metric, single-dimension result
        result = self.dt_tx.transform(mock_df.time_dim_single_metric_df, mock_df.time_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'data': 'date', 'title': 'Date'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'one', 'title': 'One'}],
            'data': [{'date': {'value': '2000-01-0%d' % (1 + i)}, 'one': {'value': i, 'display': str(i)}}
                     for i in range(8)]}, result)

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.dt_tx.transform(mock_df.time_dim_single_metric_ref_df, mock_df.time_dim_single_metric_ref_schema)
        self.assertDictEqual({
            'columns': [{'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'data': 'date', 'title': 'Date'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'one', 'title': 'One'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'wow.one',
                         'title': 'One WoW'}],
            'data': [{
                         'date': {'value': '2000-01-0%d' % (1 + i)},
                         'one': {'value': i, 'display': str(i)},
                         'wow': {'one': {'value': 2 * i, 'display': str(2 * i)}}
                     } for i in range(8)]}, result)

    def test_uni_dim_single_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and display
        result = self.dt_tx.transform(mock_df.uni_dim_single_metric_df, mock_df.uni_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [
                {'data': 'uni', 'render': {'type': 'display', '_': 'display', 'sort': 'display'}, 'title': 'Uni'},
                {'data': 'one', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'One'}],
            'data': [{'one': {'value': 0, 'display': '0'}, 'uni': {'display': 'Aa', 'value': 1}},
                     {'one': {'value': 1, 'display': '1'}, 'uni': {'display': 'Bb', 'value': 2}},
                     {'one': {'value': 2, 'display': '2'}, 'uni': {'display': 'testoption', 'value': 3}}]}, result)

    def test_uni_dim_multi_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and display
        result = self.dt_tx.transform(mock_df.uni_dim_multi_metric_df, mock_df.uni_dim_multi_metric_schema)
        self.assertDictEqual({
            'columns': [
                {'render': {'_': 'display', 'type': 'display', 'sort': 'display'}, 'title': 'Uni', 'data': 'uni'},
                {'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'One', 'data': 'one'},
                {'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'Two', 'data': 'two'}],
            'data': [{'one': {'value': 0, 'display': '0'}, 'two': {'value': 0, 'display': '0'},
                      'uni': {'value': 1, 'display': 'Aa'}},
                     {'one': {'value': 1, 'display': '1'}, 'two': {'value': 2, 'display': '2'},
                      'uni': {'value': 2, 'display': 'Bb'}},
                     {'one': {'value': 2, 'display': '2'}, 'two': {'value': 4, 'display': '4'},
                      'uni': {'value': 3, 'display': 'testoption'}}]}, result)

    def test_cat_cat_dim_single_metric(self):
        # Tests transformation of a single metric with two categorical dimensions
        result = self.dt_tx.transform(mock_df.cat_cat_dims_single_metric_df, mock_df.cat_cat_dims_single_metric_schema)
        self.assertDictEqual({
            'columns': [
                {'render': {'_': 'display', 'type': 'display', 'sort': 'display'}, 'title': 'Cat1', 'data': 'cat1'},
                {'render': {'_': 'display', 'type': 'display', 'sort': 'display'}, 'title': 'Cat2', 'data': 'cat2'},
                {'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'One', 'data': 'one'}],
            'data': [{'cat1': {'display': 'A', 'value': 'a'}, 'cat2': {'display': 'Y', 'value': 'y'},
                      'one': {'display': '0', 'value': 0}},
                     {'cat1': {'display': 'A', 'value': 'a'}, 'cat2': {'display': 'Z', 'value': 'z'},
                      'one': {'display': '1', 'value': 1}},
                     {'cat1': {'display': 'B', 'value': 'b'}, 'cat2': {'display': 'Y', 'value': 'y'},
                      'one': {'display': '2', 'value': 2}},
                     {'cat1': {'display': 'B', 'value': 'b'}, 'cat2': {'display': 'Z', 'value': 'z'},
                      'one': {'display': '3', 'value': 3}}]}, result)

    def test_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        result = self.dt_tx.transform(mock_df.cat_cat_dims_multi_metric_df, mock_df.cat_cat_dims_multi_metric_schema)

        self.assertDictEqual({
            'data': [{'one': {'value': 0, 'display': '0'}, 'two': {'value': 0, 'display': '0'},
                      'cat2': {'display': 'Y', 'value': 'y'}, 'cat1': {'display': 'A', 'value': 'a'}},
                     {'one': {'value': 1, 'display': '1'}, 'two': {'value': 2, 'display': '2'},
                      'cat2': {'display': 'Z', 'value': 'z'}, 'cat1': {'display': 'A', 'value': 'a'}},
                     {'one': {'value': 2, 'display': '2'}, 'two': {'value': 4, 'display': '4'},
                      'cat2': {'display': 'Y', 'value': 'y'}, 'cat1': {'display': 'B', 'value': 'b'}},
                     {'one': {'value': 3, 'display': '3'}, 'two': {'value': 6, 'display': '6'},
                      'cat2': {'display': 'Z', 'value': 'z'}, 'cat1': {'display': 'B', 'value': 'b'}}],
            'columns': [
                {'title': 'Cat1', 'render': {'_': 'display', 'type': 'display', 'sort': 'display'}, 'data': 'cat1'},
                {'title': 'Cat2', 'render': {'_': 'display', 'type': 'display', 'sort': 'display'}, 'data': 'cat2'},
                {'title': 'One', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'data': 'one'},
                {'title': 'Two', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'data': 'two'}]},
            result)

    def test_rollup_cont_cat_uni_dims_multi_metric_df(self):
        # Tests transformation of two metrics with a continuous, a categorical and a unique dimensions
        result = self.dt_tx.transform(mock_df.rollup_cont_cat_uni_dims_multi_metric_df,
                                      mock_df.rollup_cont_cat_uni_dims_multi_metric_schema)

        self.assertDictEqual({
            'data': [{'cont': {'value': 0}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '30', 'value': 30}, 'two': {'display': '1230', 'value': 1230},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '0', 'value': 0},
                      'two': {'display': '100', 'value': 100}, 'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '1', 'value': 1},
                      'two': {'display': '101', 'value': 101}, 'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '2', 'value': 2},
                      'two': {'display': '102', 'value': 102}, 'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '3', 'value': 3},
                      'two': {'display': '303', 'value': 303}, 'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'B', 'value': 'b'}, 'one': {'display': '3', 'value': 3},
                      'two': {'display': '103', 'value': 103}, 'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'B', 'value': 'b'}, 'one': {'display': '4', 'value': 4},
                      'two': {'display': '104', 'value': 104}, 'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'B', 'value': 'b'}, 'one': {'display': '5', 'value': 5},
                      'two': {'display': '105', 'value': 105}, 'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 0}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '12', 'value': 12}, 'two': {'display': '312', 'value': 312},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 1}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '102', 'value': 102}, 'two': {'display': '1302', 'value': 1302},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '6', 'value': 6},
                      'two': {'display': '106', 'value': 106}, 'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '7', 'value': 7},
                      'two': {'display': '107', 'value': 107}, 'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'A', 'value': 'a'}, 'one': {'display': '8', 'value': 8},
                      'two': {'display': '108', 'value': 108}, 'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '21', 'value': 21}, 'two': {'display': '321', 'value': 321},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'B', 'value': 'b'}, 'one': {'display': '9', 'value': 9},
                      'two': {'display': '109', 'value': 109}, 'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '10', 'value': 10}, 'two': {'display': '110', 'value': 110},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '11', 'value': 11}, 'two': {'display': '111', 'value': 111},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 1}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '30', 'value': 30}, 'two': {'display': '330', 'value': 330},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 2}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '174', 'value': 174}, 'two': {'display': '1374', 'value': 1374},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '12', 'value': 12}, 'two': {'display': '112', 'value': 112},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '13', 'value': 13}, 'two': {'display': '113', 'value': 113},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '14', 'value': 14}, 'two': {'display': '114', 'value': 114},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '39', 'value': 39}, 'two': {'display': '339', 'value': 339},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '15', 'value': 15}, 'two': {'display': '115', 'value': 115},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '16', 'value': 16}, 'two': {'display': '116', 'value': 116},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '17', 'value': 17}, 'two': {'display': '117', 'value': 117},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 2}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '48', 'value': 48}, 'two': {'display': '348', 'value': 348},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 3}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '246', 'value': 246}, 'two': {'display': '1446', 'value': 1446},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '18', 'value': 18}, 'two': {'display': '118', 'value': 118},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '19', 'value': 19}, 'two': {'display': '119', 'value': 119},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '20', 'value': 20}, 'two': {'display': '120', 'value': 120},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '57', 'value': 57}, 'two': {'display': '357', 'value': 357},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '21', 'value': 21}, 'two': {'display': '121', 'value': 121},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '22', 'value': 22}, 'two': {'display': '122', 'value': 122},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '23', 'value': 23}, 'two': {'display': '123', 'value': 123},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 3}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '66', 'value': 66}, 'two': {'display': '366', 'value': 366},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 4}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '318', 'value': 318}, 'two': {'display': '1518', 'value': 1518},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '24', 'value': 24}, 'two': {'display': '124', 'value': 124},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '25', 'value': 25}, 'two': {'display': '125', 'value': 125},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '26', 'value': 26}, 'two': {'display': '126', 'value': 126},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '75', 'value': 75}, 'two': {'display': '375', 'value': 375},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '27', 'value': 27}, 'two': {'display': '127', 'value': 127},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '28', 'value': 28}, 'two': {'display': '128', 'value': 128},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '29', 'value': 29}, 'two': {'display': '129', 'value': 129},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 4}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '84', 'value': 84}, 'two': {'display': '384', 'value': 384},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 5}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '390', 'value': 390}, 'two': {'display': '1590', 'value': 1590},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '30', 'value': 30}, 'two': {'display': '130', 'value': 130},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '31', 'value': 31}, 'two': {'display': '131', 'value': 131},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '32', 'value': 32}, 'two': {'display': '132', 'value': 132},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '93', 'value': 93}, 'two': {'display': '393', 'value': 393},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '33', 'value': 33}, 'two': {'display': '133', 'value': 133},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '34', 'value': 34}, 'two': {'display': '134', 'value': 134},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '35', 'value': 35}, 'two': {'display': '135', 'value': 135},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 5}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '102', 'value': 102}, 'two': {'display': '402', 'value': 402},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 6}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '462', 'value': 462}, 'two': {'display': '1662', 'value': 1662},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '36', 'value': 36}, 'two': {'display': '136', 'value': 136},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '37', 'value': 37}, 'two': {'display': '137', 'value': 137},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '38', 'value': 38}, 'two': {'display': '138', 'value': 138},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '111', 'value': 111}, 'two': {'display': '411', 'value': 411},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '39', 'value': 39}, 'two': {'display': '139', 'value': 139},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '40', 'value': 40}, 'two': {'display': '140', 'value': 140},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '41', 'value': 41}, 'two': {'display': '141', 'value': 141},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 6}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '120', 'value': 120}, 'two': {'display': '420', 'value': 420},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 7}, 'cat1': {'display': '_total', 'value': '_total'},
                      'one': {'display': '534', 'value': 534}, 'two': {'display': '1734', 'value': 1734},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '42', 'value': 42}, 'two': {'display': '142', 'value': 142},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '43', 'value': 43}, 'two': {'display': '143', 'value': 143},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '44', 'value': 44}, 'two': {'display': '144', 'value': 144},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'A', 'value': 'a'},
                      'one': {'display': '129', 'value': 129}, 'two': {'display': '429', 'value': 429},
                      'uni': {'display': '_total', 'value': '_total'}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '45', 'value': 45}, 'two': {'display': '145', 'value': 145},
                      'uni': {'display': 'Aa', 'value': 1}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '46', 'value': 46}, 'two': {'display': '146', 'value': 146},
                      'uni': {'display': 'Bb', 'value': 2}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '47', 'value': 47}, 'two': {'display': '147', 'value': 147},
                      'uni': {'display': 'testoption', 'value': 3}},
                     {'cont': {'value': 7}, 'cat1': {'display': 'B', 'value': 'b'},
                      'one': {'display': '138', 'value': 138}, 'two': {'display': '438', 'value': 438},
                      'uni': {'display': '_total', 'value': '_total'}}],
            'columns': [
                {'render': {'sort': 'value', '_': 'value', 'type': 'value'}, 'title': 'Cont', 'data': 'cont'},
                {'render': {'sort': 'display', '_': 'display', 'type': 'display'}, 'title': 'Cat1', 'data': 'cat1'},
                {'render': {'sort': 'display', '_': 'display', 'type': 'display'}, 'title': 'Uni', 'data': 'uni'},
                {'render': {'sort': 'value', '_': 'display', 'type': 'value'}, 'title': 'One', 'data': 'one'},
                {'render': {'sort': 'value', '_': 'display', 'type': 'value'}, 'title': 'Two', 'data': 'two'}]},
            result)

    def test_rollup_cont_cat_cat_dims_multi_metric_df(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(mock_df.rollup_cont_cat_cat_dims_multi_metric_df,
                                      mock_df.rollup_cont_cat_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'title': 'Cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'data': 'cont'},
                        {'title': 'Cat1', 'render': {'type': 'display', '_': 'display', 'sort': 'display'},
                         'data': 'cat1'},
                        {'title': 'Cat2', 'render': {'type': 'display', '_': 'display', 'sort': 'display'},
                         'data': 'cat2'},
                        {'title': 'One', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'one'},
                        {'title': 'Two', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'data': 'two'}],
            'data': [
                {'one': {'value': 12, 'display': '12'}, 'two': {'value': 24, 'display': '24'}, 'cont': {'value': 0},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 1, 'display': '1'}, 'two': {'value': 2, 'display': '2'}, 'cont': {'value': 0},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 0, 'display': '0'}, 'two': {'value': 0, 'display': '0'}, 'cont': {'value': 0},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 1, 'display': '1'}, 'two': {'value': 2, 'display': '2'}, 'cont': {'value': 0},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 5, 'display': '5'}, 'two': {'value': 10, 'display': '10'}, 'cont': {'value': 0},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 2, 'display': '2'}, 'two': {'value': 4, 'display': '4'}, 'cont': {'value': 0},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 3, 'display': '3'}, 'two': {'value': 6, 'display': '6'}, 'cont': {'value': 0},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 44, 'display': '44'}, 'two': {'value': 88, 'display': '88'}, 'cont': {'value': 1},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 9, 'display': '9'}, 'two': {'value': 18, 'display': '18'}, 'cont': {'value': 1},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 4, 'display': '4'}, 'two': {'value': 8, 'display': '8'}, 'cont': {'value': 1},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 5, 'display': '5'}, 'two': {'value': 10, 'display': '10'}, 'cont': {'value': 1},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 13, 'display': '13'}, 'two': {'value': 26, 'display': '26'}, 'cont': {'value': 1},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 6, 'display': '6'}, 'two': {'value': 12, 'display': '12'}, 'cont': {'value': 1},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 7, 'display': '7'}, 'two': {'value': 14, 'display': '14'}, 'cont': {'value': 1},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 76, 'display': '76'}, 'two': {'value': 152, 'display': '152'}, 'cont': {'value': 2},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 17, 'display': '17'}, 'two': {'value': 34, 'display': '34'}, 'cont': {'value': 2},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 8, 'display': '8'}, 'two': {'value': 16, 'display': '16'}, 'cont': {'value': 2},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 9, 'display': '9'}, 'two': {'value': 18, 'display': '18'}, 'cont': {'value': 2},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 21, 'display': '21'}, 'two': {'value': 42, 'display': '42'}, 'cont': {'value': 2},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 10, 'display': '10'}, 'two': {'value': 20, 'display': '20'}, 'cont': {'value': 2},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 11, 'display': '11'}, 'two': {'value': 22, 'display': '22'}, 'cont': {'value': 2},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 108, 'display': '108'}, 'two': {'value': 216, 'display': '216'}, 'cont': {'value': 3},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 25, 'display': '25'}, 'two': {'value': 50, 'display': '50'}, 'cont': {'value': 3},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 12, 'display': '12'}, 'two': {'value': 24, 'display': '24'}, 'cont': {'value': 3},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 13, 'display': '13'}, 'two': {'value': 26, 'display': '26'}, 'cont': {'value': 3},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 29, 'display': '29'}, 'two': {'value': 58, 'display': '58'}, 'cont': {'value': 3},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 14, 'display': '14'}, 'two': {'value': 28, 'display': '28'}, 'cont': {'value': 3},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 15, 'display': '15'}, 'two': {'value': 30, 'display': '30'}, 'cont': {'value': 3},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 140, 'display': '140'}, 'two': {'value': 280, 'display': '280'}, 'cont': {'value': 4},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 33, 'display': '33'}, 'two': {'value': 66, 'display': '66'}, 'cont': {'value': 4},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 16, 'display': '16'}, 'two': {'value': 32, 'display': '32'}, 'cont': {'value': 4},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 17, 'display': '17'}, 'two': {'value': 34, 'display': '34'}, 'cont': {'value': 4},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 37, 'display': '37'}, 'two': {'value': 74, 'display': '74'}, 'cont': {'value': 4},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 18, 'display': '18'}, 'two': {'value': 36, 'display': '36'}, 'cont': {'value': 4},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 19, 'display': '19'}, 'two': {'value': 38, 'display': '38'}, 'cont': {'value': 4},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 172, 'display': '172'}, 'two': {'value': 344, 'display': '344'}, 'cont': {'value': 5},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 41, 'display': '41'}, 'two': {'value': 82, 'display': '82'}, 'cont': {'value': 5},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 20, 'display': '20'}, 'two': {'value': 40, 'display': '40'}, 'cont': {'value': 5},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 21, 'display': '21'}, 'two': {'value': 42, 'display': '42'}, 'cont': {'value': 5},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 45, 'display': '45'}, 'two': {'value': 90, 'display': '90'}, 'cont': {'value': 5},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 22, 'display': '22'}, 'two': {'value': 44, 'display': '44'}, 'cont': {'value': 5},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 23, 'display': '23'}, 'two': {'value': 46, 'display': '46'}, 'cont': {'value': 5},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 204, 'display': '204'}, 'two': {'value': 408, 'display': '408'}, 'cont': {'value': 6},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 49, 'display': '49'}, 'two': {'value': 98, 'display': '98'}, 'cont': {'value': 6},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 24, 'display': '24'}, 'two': {'value': 48, 'display': '48'}, 'cont': {'value': 6},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 25, 'display': '25'}, 'two': {'value': 50, 'display': '50'}, 'cont': {'value': 6},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 53, 'display': '53'}, 'two': {'value': 106, 'display': '106'}, 'cont': {'value': 6},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 26, 'display': '26'}, 'two': {'value': 52, 'display': '52'}, 'cont': {'value': 6},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 27, 'display': '27'}, 'two': {'value': 54, 'display': '54'}, 'cont': {'value': 6},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 236, 'display': '236'}, 'two': {'value': 472, 'display': '472'}, 'cont': {'value': 7},
                 'cat1': {'value': Totals.key, 'display': Totals.label},
                 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 57, 'display': '57'}, 'two': {'value': 114, 'display': '114'}, 'cont': {'value': 7},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 28, 'display': '28'}, 'two': {'value': 56, 'display': '56'}, 'cont': {'value': 7},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 29, 'display': '29'}, 'two': {'value': 58, 'display': '58'}, 'cont': {'value': 7},
                 'cat1': {'value': 'a', 'display': 'A'}, 'cat2': {'value': 'z', 'display': 'Z'}},
                {'one': {'value': 61, 'display': '61'}, 'two': {'value': 122, 'display': '122'}, 'cont': {'value': 7},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': Totals.key, 'display': Totals.label}},
                {'one': {'value': 30, 'display': '30'}, 'two': {'value': 60, 'display': '60'}, 'cont': {'value': 7},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'y', 'display': 'Y'}},
                {'one': {'value': 31, 'display': '31'}, 'two': {'value': 62, 'display': '62'}, 'cont': {'value': 7},
                 'cat1': {'value': 'b', 'display': 'B'}, 'cat2': {'value': 'z', 'display': 'Z'}}]}
            , result)


class DataTablesColumnIndexTransformerTests(TestCase):
    maxDiff = None
    dt_tx = DataTablesColumnIndexTransformer()

    def test_no_dims_multi_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(mock_df.no_dims_multi_metric_df, mock_df.no_dims_multi_metric_schema)
        self.assertDictEqual({
            'data': [{'eight': {'value': 7, 'display': '7'}, 'one': {'value': 0, 'display': '0'},
                      'seven': {'value': 6, 'display': '6'}, 'four': {'value': 3, 'display': '3'},
                      'three': {'value': 2, 'display': '2'}, 'five': {'value': 4, 'display': '4'},
                      'six': {'value': 5, 'display': '5'}, 'two': {'value': 1, 'display': '1'}}],
            'columns': [{'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'One', 'data': 'one'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Two', 'data': 'two'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Three',
                         'data': 'three'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Four', 'data': 'four'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Five', 'data': 'five'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Six', 'data': 'six'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Seven',
                         'data': 'seven'},
                        {'render': {'type': 'value', '_': 'display', 'sort': 'value'}, 'title': 'Eight',
                         'data': 'eight'}]}, result)

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        result = self.dt_tx.transform(mock_df.cont_dim_single_metric_df, mock_df.cont_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'title': 'Cont', 'data': 'cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}},
                        {'title': 'One', 'data': 'one', 'render': {'type': 'value', '_': 'display', 'sort': 'value'}}],
            'data': [{
                         'cont': {'value': i},
                         'one': {'value': i, 'display': str(i)},
                     } for i in range(8)]}, result)

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        result = self.dt_tx.transform(mock_df.cont_dim_multi_metric_df, mock_df.cont_dim_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'title': 'Cont'},
                        {'data': 'one', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'One'},
                        {'data': 'two', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'Two'}],
            'data': [{
                         'cont': {'value': i},
                         'one': {'value': i, 'display': str(i)},
                         'two': {'value': 2 * i, 'display': str(2 * i)},
                     } for i in range(8)]}, result)

    def test_time_series_date_to_millis(self):
        # Tests transformation of a single-metric, single-dimension result
        result = self.dt_tx.transform(mock_df.time_dim_single_metric_df, mock_df.time_dim_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'date', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'title': 'Date'},
                        {'data': 'one', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'One'}],
            'data': [{
                         'date': {'value': '2000-01-0%d' % (i + 1)},
                         'one': {'value': i, 'display': str(i)}
                     } for i in range(8)]}, result)

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        result = self.dt_tx.transform(mock_df.time_dim_single_metric_ref_df, mock_df.time_dim_single_metric_ref_schema)
        self.assertDictEqual({
            'columns': [{'data': 'date', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'title': 'Date'},
                        {'data': 'one', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'}, 'title': 'One'},
                        {'data': 'wow.one', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'},
                         'title': 'One WoW'}],
            'data': [{
                         'date': {'value': '2000-01-0%d' % (i + 1)},
                         'one': {'value': i, 'display': str(i)},
                         'wow': {'one': {'value': 2 * i, 'display': str(2 * i)}}
                     } for i in range(8)]}, result)

    def test_cont_cat_dim_single_metric(self):
        # Tests transformation of a single metric with a continuous and a categorical dimension
        result = self.dt_tx.transform(mock_df.cont_cat_dims_single_metric_df,
                                      mock_df.cont_cat_dims_single_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont', 'title': 'Cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}},
                        {'data': 'a.one', 'title': 'One (A)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'b.one', 'title': 'One (B)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}}],
            'data': [{'b': {'one': {'value': 1, 'display': '1'}}, 'a': {'one': {'value': 0, 'display': '0'}},
                      'cont': {'value': 0}},
                     {'b': {'one': {'value': 3, 'display': '3'}}, 'a': {'one': {'value': 2, 'display': '2'}},
                      'cont': {'value': 1}},
                     {'b': {'one': {'value': 5, 'display': '5'}}, 'a': {'one': {'value': 4, 'display': '4'}},
                      'cont': {'value': 2}},
                     {'b': {'one': {'value': 7, 'display': '7'}}, 'a': {'one': {'value': 6, 'display': '6'}},
                      'cont': {'value': 3}},
                     {'b': {'one': {'value': 9, 'display': '9'}}, 'a': {'one': {'value': 8, 'display': '8'}},
                      'cont': {'value': 4}},
                     {'b': {'one': {'value': 11, 'display': '11'}}, 'a': {'one': {'value': 10, 'display': '10'}},
                      'cont': {'value': 5}},
                     {'b': {'one': {'value': 13, 'display': '13'}}, 'a': {'one': {'value': 12, 'display': '12'}},
                      'cont': {'value': 6}},
                     {'b': {'one': {'value': 15, 'display': '15'}}, 'a': {'one': {'value': 14, 'display': '14'}},
                      'cont': {'value': 7}}]}
            , result)

    def test_cont_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and a categorical dimension
        result = self.dt_tx.transform(mock_df.cont_cat_dims_multi_metric_df, mock_df.cont_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'data': [{'b': {'one': {'display': '1', 'value': 1}, 'two': {'display': '2', 'value': 2}},
                      'a': {'one': {'display': '0', 'value': 0}, 'two': {'display': '0', 'value': 0}},
                      'cont': {'value': 0}},
                     {'b': {'one': {'display': '3', 'value': 3}, 'two': {'display': '6', 'value': 6}},
                      'a': {'one': {'display': '2', 'value': 2}, 'two': {'display': '4', 'value': 4}},
                      'cont': {'value': 1}},
                     {'b': {'one': {'display': '5', 'value': 5}, 'two': {'display': '10', 'value': 10}},
                      'a': {'one': {'display': '4', 'value': 4}, 'two': {'display': '8', 'value': 8}},
                      'cont': {'value': 2}},
                     {'b': {'one': {'display': '7', 'value': 7}, 'two': {'display': '14', 'value': 14}},
                      'a': {'one': {'display': '6', 'value': 6}, 'two': {'display': '12', 'value': 12}},
                      'cont': {'value': 3}},
                     {'b': {'one': {'display': '9', 'value': 9}, 'two': {'display': '18', 'value': 18}},
                      'a': {'one': {'display': '8', 'value': 8}, 'two': {'display': '16', 'value': 16}},
                      'cont': {'value': 4}},
                     {'b': {'one': {'display': '11', 'value': 11}, 'two': {'display': '22', 'value': 22}},
                      'a': {'one': {'display': '10', 'value': 10}, 'two': {'display': '20', 'value': 20}},
                      'cont': {'value': 5}},
                     {'b': {'one': {'display': '13', 'value': 13}, 'two': {'display': '26', 'value': 26}},
                      'a': {'one': {'display': '12', 'value': 12}, 'two': {'display': '24', 'value': 24}},
                      'cont': {'value': 6}},
                     {'b': {'one': {'display': '15', 'value': 15}, 'two': {'display': '30', 'value': 30}},
                      'a': {'one': {'display': '14', 'value': 14}, 'two': {'display': '28', 'value': 28}},
                      'cont': {'value': 7}}],
            'columns': [{'title': 'Cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}, 'data': 'cont'},
                        {'title': 'One (A)', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'},
                         'data': 'a.one'},
                        {'title': 'One (B)', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'},
                         'data': 'b.one'},
                        {'title': 'Two (A)', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'},
                         'data': 'a.two'},
                        {'title': 'Two (B)', 'render': {'_': 'display', 'type': 'value', 'sort': 'value'},
                         'data': 'b.two'}]}, result)

    def test_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(mock_df.cont_cat_cat_dims_multi_metric_df,
                                      mock_df.cont_cat_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'data': [{'b': {'z': {'one': {'value': 3, 'display': '3'}, 'two': {'value': 6, 'display': '6'}},
                            'y': {'one': {'value': 2, 'display': '2'}, 'two': {'value': 4, 'display': '4'}}},
                      'a': {'z': {'one': {'value': 1, 'display': '1'}, 'two': {'value': 2, 'display': '2'}},
                            'y': {'one': {'value': 0, 'display': '0'}, 'two': {'value': 0, 'display': '0'}}},
                      'cont': {'value': 0}}, {
                         'b': {'z': {'one': {'value': 7, 'display': '7'}, 'two': {'value': 14, 'display': '14'}},
                               'y': {'one': {'value': 6, 'display': '6'}, 'two': {'value': 12, 'display': '12'}}},
                         'a': {'z': {'one': {'value': 5, 'display': '5'}, 'two': {'value': 10, 'display': '10'}},
                               'y': {'one': {'value': 4, 'display': '4'}, 'two': {'value': 8, 'display': '8'}}},
                         'cont': {'value': 1}}, {
                         'b': {'z': {'one': {'value': 11, 'display': '11'}, 'two': {'value': 22, 'display': '22'}},
                               'y': {'one': {'value': 10, 'display': '10'}, 'two': {'value': 20, 'display': '20'}}},
                         'a': {'z': {'one': {'value': 9, 'display': '9'}, 'two': {'value': 18, 'display': '18'}},
                               'y': {'one': {'value': 8, 'display': '8'}, 'two': {'value': 16, 'display': '16'}}},
                         'cont': {'value': 2}}, {
                         'b': {'z': {'one': {'value': 15, 'display': '15'}, 'two': {'value': 30, 'display': '30'}},
                               'y': {'one': {'value': 14, 'display': '14'}, 'two': {'value': 28, 'display': '28'}}},
                         'a': {'z': {'one': {'value': 13, 'display': '13'}, 'two': {'value': 26, 'display': '26'}},
                               'y': {'one': {'value': 12, 'display': '12'}, 'two': {'value': 24, 'display': '24'}}},
                         'cont': {'value': 3}}, {
                         'b': {'z': {'one': {'value': 19, 'display': '19'}, 'two': {'value': 38, 'display': '38'}},
                               'y': {'one': {'value': 18, 'display': '18'}, 'two': {'value': 36, 'display': '36'}}},
                         'a': {'z': {'one': {'value': 17, 'display': '17'}, 'two': {'value': 34, 'display': '34'}},
                               'y': {'one': {'value': 16, 'display': '16'}, 'two': {'value': 32, 'display': '32'}}},
                         'cont': {'value': 4}}, {
                         'b': {'z': {'one': {'value': 23, 'display': '23'}, 'two': {'value': 46, 'display': '46'}},
                               'y': {'one': {'value': 22, 'display': '22'}, 'two': {'value': 44, 'display': '44'}}},
                         'a': {'z': {'one': {'value': 21, 'display': '21'}, 'two': {'value': 42, 'display': '42'}},
                               'y': {'one': {'value': 20, 'display': '20'}, 'two': {'value': 40, 'display': '40'}}},
                         'cont': {'value': 5}}, {
                         'b': {'z': {'one': {'value': 27, 'display': '27'}, 'two': {'value': 54, 'display': '54'}},
                               'y': {'one': {'value': 26, 'display': '26'}, 'two': {'value': 52, 'display': '52'}}},
                         'a': {'z': {'one': {'value': 25, 'display': '25'}, 'two': {'value': 50, 'display': '50'}},
                               'y': {'one': {'value': 24, 'display': '24'}, 'two': {'value': 48, 'display': '48'}}},
                         'cont': {'value': 6}}, {
                         'b': {'z': {'one': {'value': 31, 'display': '31'}, 'two': {'value': 62, 'display': '62'}},
                               'y': {'one': {'value': 30, 'display': '30'}, 'two': {'value': 60, 'display': '60'}}},
                         'a': {'z': {'one': {'value': 29, 'display': '29'}, 'two': {'value': 58, 'display': '58'}},
                               'y': {'one': {'value': 28, 'display': '28'}, 'two': {'value': 56, 'display': '56'}}},
                         'cont': {'value': 7}}],
            'columns': [{'data': 'cont', 'title': 'Cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}},
                        {'data': 'a.y.one', 'title': 'One (A, Y)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'a.z.one', 'title': 'One (A, Z)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'b.y.one', 'title': 'One (B, Y)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'b.z.one', 'title': 'One (B, Z)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'a.y.two', 'title': 'Two (A, Y)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'a.z.two', 'title': 'Two (A, Z)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'b.y.two', 'title': 'Two (B, Y)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}},
                        {'data': 'b.z.two', 'title': 'Two (B, Z)',
                         'render': {'type': 'value', '_': 'display', 'sort': 'value'}}]},
            result)

    def test_cont_cat_uni_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(mock_df.cont_cat_uni_dims_multi_metric_df,
                                      mock_df.cont_cat_uni_dims_multi_metric_schema)
        self.assertDictEqual({
            'data': [{'cont': {'value': 0},
                      'b': {1: {'one': {'display': '3', 'value': 3}, 'two': {'display': '103', 'value': 103}},
                            2: {'one': {'display': '4', 'value': 4}, 'two': {'display': '104', 'value': 104}},
                            3: {'one': {'display': '5', 'value': 5}, 'two': {'display': '105', 'value': 105}}},
                      'a': {1: {'one': {'display': '0', 'value': 0}, 'two': {'display': '100', 'value': 100}},
                            2: {'one': {'display': '1', 'value': 1}, 'two': {'display': '101', 'value': 101}},
                            3: {'one': {'display': '2', 'value': 2}, 'two': {'display': '102', 'value': 102}}}},
                     {'cont': {'value': 1},
                      'b': {1: {'one': {'display': '9', 'value': 9}, 'two': {'display': '109', 'value': 109}},
                            2: {'one': {'display': '10', 'value': 10}, 'two': {'display': '110', 'value': 110}},
                            3: {'one': {'display': '11', 'value': 11}, 'two': {'display': '111', 'value': 111}}},
                      'a': {1: {'one': {'display': '6', 'value': 6}, 'two': {'display': '106', 'value': 106}},
                            2: {'one': {'display': '7', 'value': 7}, 'two': {'display': '107', 'value': 107}},
                            3: {'one': {'display': '8', 'value': 8}, 'two': {'display': '108', 'value': 108}}}},
                     {'cont': {'value': 2},
                      'b': {1: {'one': {'display': '15', 'value': 15}, 'two': {'display': '115', 'value': 115}},
                            2: {'one': {'display': '16', 'value': 16}, 'two': {'display': '116', 'value': 116}},
                            3: {'one': {'display': '17', 'value': 17}, 'two': {'display': '117', 'value': 117}}},
                      'a': {1: {'one': {'display': '12', 'value': 12}, 'two': {'display': '112', 'value': 112}},
                            2: {'one': {'display': '13', 'value': 13}, 'two': {'display': '113', 'value': 113}},
                            3: {'one': {'display': '14', 'value': 14}, 'two': {'display': '114', 'value': 114}}}},
                     {'cont': {'value': 3},
                      'b': {1: {'one': {'display': '21', 'value': 21}, 'two': {'display': '121', 'value': 121}},
                            2: {'one': {'display': '22', 'value': 22}, 'two': {'display': '122', 'value': 122}},
                            3: {'one': {'display': '23', 'value': 23}, 'two': {'display': '123', 'value': 123}}},
                      'a': {1: {'one': {'display': '18', 'value': 18}, 'two': {'display': '118', 'value': 118}},
                            2: {'one': {'display': '19', 'value': 19}, 'two': {'display': '119', 'value': 119}},
                            3: {'one': {'display': '20', 'value': 20}, 'two': {'display': '120', 'value': 120}}}},
                     {'cont': {'value': 4},
                      'b': {1: {'one': {'display': '27', 'value': 27}, 'two': {'display': '127', 'value': 127}},
                            2: {'one': {'display': '28', 'value': 28}, 'two': {'display': '128', 'value': 128}},
                            3: {'one': {'display': '29', 'value': 29}, 'two': {'display': '129', 'value': 129}}},
                      'a': {1: {'one': {'display': '24', 'value': 24}, 'two': {'display': '124', 'value': 124}},
                            2: {'one': {'display': '25', 'value': 25}, 'two': {'display': '125', 'value': 125}},
                            3: {'one': {'display': '26', 'value': 26}, 'two': {'display': '126', 'value': 126}}}},
                     {'cont': {'value': 5},
                      'b': {1: {'one': {'display': '33', 'value': 33}, 'two': {'display': '133', 'value': 133}},
                            2: {'one': {'display': '34', 'value': 34}, 'two': {'display': '134', 'value': 134}},
                            3: {'one': {'display': '35', 'value': 35}, 'two': {'display': '135', 'value': 135}}},
                      'a': {1: {'one': {'display': '30', 'value': 30}, 'two': {'display': '130', 'value': 130}},
                            2: {'one': {'display': '31', 'value': 31}, 'two': {'display': '131', 'value': 131}},
                            3: {'one': {'display': '32', 'value': 32}, 'two': {'display': '132', 'value': 132}}}},
                     {'cont': {'value': 6},
                      'b': {1: {'one': {'display': '39', 'value': 39}, 'two': {'display': '139', 'value': 139}},
                            2: {'one': {'display': '40', 'value': 40}, 'two': {'display': '140', 'value': 140}},
                            3: {'one': {'display': '41', 'value': 41}, 'two': {'display': '141', 'value': 141}}},
                      'a': {1: {'one': {'display': '36', 'value': 36}, 'two': {'display': '136', 'value': 136}},
                            2: {'one': {'display': '37', 'value': 37}, 'two': {'display': '137', 'value': 137}},
                            3: {'one': {'display': '38', 'value': 38}, 'two': {'display': '138', 'value': 138}}}},
                     {'cont': {'value': 7},
                      'b': {1: {'one': {'display': '45', 'value': 45}, 'two': {'display': '145', 'value': 145}},
                            2: {'one': {'display': '46', 'value': 46}, 'two': {'display': '146', 'value': 146}},
                            3: {'one': {'display': '47', 'value': 47}, 'two': {'display': '147', 'value': 147}}},
                      'a': {1: {'one': {'display': '42', 'value': 42}, 'two': {'display': '142', 'value': 142}},
                            2: {'one': {'display': '43', 'value': 43}, 'two': {'display': '143', 'value': 143}},
                            3: {'one': {'display': '44', 'value': 44}, 'two': {'display': '144', 'value': 144}}}}],
            'columns': [{'data': 'cont', 'title': 'Cont', 'render': {'_': 'value', 'sort': 'value', 'type': 'value'}},
                        {'data': 'a.1.one', 'title': 'One (A, Aa)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.2.one', 'title': 'One (A, Bb)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.3.one', 'title': 'One (A, testoption)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.1.one', 'title': 'One (B, Aa)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.2.one', 'title': 'One (B, Bb)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.3.one', 'title': 'One (B, testoption)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.1.two', 'title': 'Two (A, Aa)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.2.two', 'title': 'Two (A, Bb)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.3.two', 'title': 'Two (A, testoption)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.1.two', 'title': 'Two (B, Aa)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.2.two', 'title': 'Two (B, Bb)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.3.two', 'title': 'Two (B, testoption)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}}]},
            result)

    def test_rollup_cont_cat_uni_dims_multi_metric_df(self):
        # Tests transformation of two metrics with a continuous, a categorical and a unique dimensions
        result = self.dt_tx.transform(mock_df.rollup_cont_cat_uni_dims_multi_metric_df,
                                      mock_df.rollup_cont_cat_uni_dims_multi_metric_schema)

        self.assertDictEqual({'data': [
            {'cont': {'value': 0},
             '_total': {'_total': {'one': {'display': '30', 'value': 30}, 'two': {'display': '1230', 'value': 1230}}},
             'b': {1: {'one': {'display': '3', 'value': 3}, 'two': {'display': '103', 'value': 103}},
                   2: {'one': {'display': '4', 'value': 4}, 'two': {'display': '104', 'value': 104}},
                   3: {'one': {'display': '5', 'value': 5}, 'two': {'display': '105', 'value': 105}},
                   '_total': {'one': {'display': '12', 'value': 12}, 'two': {'display': '312', 'value': 312}}},
             'a': {1: {'one': {'display': '0', 'value': 0}, 'two': {'display': '100', 'value': 100}},
                   2: {'one': {'display': '1', 'value': 1}, 'two': {'display': '101', 'value': 101}},
                   3: {'one': {'display': '2', 'value': 2}, 'two': {'display': '102', 'value': 102}},
                   '_total': {'one': {'display': '3', 'value': 3}, 'two': {'display': '303', 'value': 303}}}},

            {'cont': {'value': 1},
             '_total': {'_total': {'one': {'display': '102', 'value': 102}, 'two': {'display': '1302', 'value': 1302}}},
             'b': {1: {'one': {'display': '9', 'value': 9}, 'two': {'display': '109', 'value': 109}},
                   2: {'one': {'display': '10', 'value': 10}, 'two': {'display': '110', 'value': 110}},
                   3: {'one': {'display': '11', 'value': 11}, 'two': {'display': '111', 'value': 111}},
                   '_total': {'one': {'display': '30', 'value': 30}, 'two': {'display': '330', 'value': 330}}},
             'a': {1: {'one': {'display': '6', 'value': 6}, 'two': {'display': '106', 'value': 106}},
                   2: {'one': {'display': '7', 'value': 7}, 'two': {'display': '107', 'value': 107}},
                   3: {'one': {'display': '8', 'value': 8}, 'two': {'display': '108', 'value': 108}},
                   '_total': {'one': {'display': '21', 'value': 21}, 'two': {'display': '321', 'value': 321}}}},
            {'cont': {'value': 2},
             '_total': {'_total': {'one': {'display': '174', 'value': 174}, 'two': {'display': '1374', 'value': 1374}}},
             'b': {1: {'one': {'display': '15', 'value': 15}, 'two': {'display': '115', 'value': 115}},
                   2: {'one': {'display': '16', 'value': 16}, 'two': {'display': '116', 'value': 116}},
                   3: {'one': {'display': '17', 'value': 17}, 'two': {'display': '117', 'value': 117}},
                   '_total': {'one': {'display': '48', 'value': 48}, 'two': {'display': '348', 'value': 348}}},
             'a': {1: {'one': {'display': '12', 'value': 12}, 'two': {'display': '112', 'value': 112}},
                   2: {'one': {'display': '13', 'value': 13}, 'two': {'display': '113', 'value': 113}},
                   3: {'one': {'display': '14', 'value': 14}, 'two': {'display': '114', 'value': 114}},
                   '_total': {'one': {'display': '39', 'value': 39}, 'two': {'display': '339', 'value': 339}}}},
            {'cont': {'value': 3},
             '_total': {'_total': {'one': {'display': '246', 'value': 246}, 'two': {'display': '1446', 'value': 1446}}},
             'b': {1: {'one': {'display': '21', 'value': 21}, 'two': {'display': '121', 'value': 121}},
                   2: {'one': {'display': '22', 'value': 22}, 'two': {'display': '122', 'value': 122}},
                   3: {'one': {'display': '23', 'value': 23}, 'two': {'display': '123', 'value': 123}},
                   '_total': {'one': {'display': '66', 'value': 66}, 'two': {'display': '366', 'value': 366}}},
             'a': {1: {'one': {'display': '18', 'value': 18}, 'two': {'display': '118', 'value': 118}},
                   2: {'one': {'display': '19', 'value': 19}, 'two': {'display': '119', 'value': 119}},
                   3: {'one': {'display': '20', 'value': 20}, 'two': {'display': '120', 'value': 120}},
                   '_total': {'one': {'display': '57', 'value': 57}, 'two': {'display': '357', 'value': 357}}}},
            {'cont': {'value': 4},
             '_total': {'_total': {'one': {'display': '318', 'value': 318}, 'two': {'display': '1518', 'value': 1518}}},
             'b': {1: {'one': {'display': '27', 'value': 27}, 'two': {'display': '127', 'value': 127}},
                   2: {'one': {'display': '28', 'value': 28}, 'two': {'display': '128', 'value': 128}},
                   3: {'one': {'display': '29', 'value': 29}, 'two': {'display': '129', 'value': 129}},
                   '_total': {'one': {'display': '84', 'value': 84}, 'two': {'display': '384', 'value': 384}}},
             'a': {1: {'one': {'display': '24', 'value': 24}, 'two': {'display': '124', 'value': 124}},
                   2: {'one': {'display': '25', 'value': 25}, 'two': {'display': '125', 'value': 125}},
                   3: {'one': {'display': '26', 'value': 26}, 'two': {'display': '126', 'value': 126}},
                   '_total': {'one': {'display': '75', 'value': 75}, 'two': {'display': '375', 'value': 375}}}},
            {'cont': {'value': 5},
             '_total': {'_total': {'one': {'display': '390', 'value': 390}, 'two': {'display': '1590', 'value': 1590}}},
             'b': {1: {'one': {'display': '33', 'value': 33}, 'two': {'display': '133', 'value': 133}},
                   2: {'one': {'display': '34', 'value': 34}, 'two': {'display': '134', 'value': 134}},
                   3: {'one': {'display': '35', 'value': 35}, 'two': {'display': '135', 'value': 135}},
                   '_total': {'one': {'display': '102', 'value': 102}, 'two': {'display': '402', 'value': 402}}},
             'a': {1: {'one': {'display': '30', 'value': 30}, 'two': {'display': '130', 'value': 130}},
                   2: {'one': {'display': '31', 'value': 31}, 'two': {'display': '131', 'value': 131}},
                   3: {'one': {'display': '32', 'value': 32}, 'two': {'display': '132', 'value': 132}},
                   '_total': {'one': {'display': '93', 'value': 93}, 'two': {'display': '393', 'value': 393}}}},
            {'cont': {'value': 6},
             '_total': {'_total': {'one': {'display': '462', 'value': 462}, 'two': {'display': '1662', 'value': 1662}}},
             'b': {1: {'one': {'display': '39', 'value': 39}, 'two': {'display': '139', 'value': 139}},
                   2: {'one': {'display': '40', 'value': 40}, 'two': {'display': '140', 'value': 140}},
                   3: {'one': {'display': '41', 'value': 41}, 'two': {'display': '141', 'value': 141}},
                   '_total': {'one': {'display': '120', 'value': 120}, 'two': {'display': '420', 'value': 420}}},
             'a': {1: {'one': {'display': '36', 'value': 36}, 'two': {'display': '136', 'value': 136}},
                   2: {'one': {'display': '37', 'value': 37}, 'two': {'display': '137', 'value': 137}},
                   3: {'one': {'display': '38', 'value': 38}, 'two': {'display': '138', 'value': 138}},
                   '_total': {'one': {'display': '111', 'value': 111}, 'two': {'display': '411', 'value': 411}}}},
            {'cont': {'value': 7},
             '_total': {'_total': {'one': {'display': '534', 'value': 534}, 'two': {'display': '1734', 'value': 1734}}},
             'b': {1: {'one': {'display': '45', 'value': 45}, 'two': {'display': '145', 'value': 145}},
                   2: {'one': {'display': '46', 'value': 46}, 'two': {'display': '146', 'value': 146}},
                   3: {'one': {'display': '47', 'value': 47}, 'two': {'display': '147', 'value': 147}},
                   '_total': {'one': {'display': '138', 'value': 138}, 'two': {'display': '438', 'value': 438}}},
             'a': {1: {'one': {'display': '42', 'value': 42}, 'two': {'display': '142', 'value': 142}},
                   2: {'one': {'display': '43', 'value': 43}, 'two': {'display': '143', 'value': 143}},
                   3: {'one': {'display': '44', 'value': 44}, 'two': {'display': '144', 'value': 144}},
                   '_total': {'one': {'display': '129', 'value': 129}, 'two': {'display': '429', 'value': 429}}}}],
            'columns': [{'title': 'Cont', 'data': 'cont', 'render': {'_': 'value', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One', 'data': '_total._total.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (A, Aa)', 'data': 'a.1.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (A, Bb)', 'data': 'a.2.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (A, testoption)', 'data': 'a.3.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (A)', 'data': 'a._total.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (B, Aa)', 'data': 'b.1.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (B, Bb)', 'data': 'b.2.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (B, testoption)', 'data': 'b.3.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'One (B)', 'data': 'b._total.one',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two', 'data': '_total._total.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (A, Aa)', 'data': 'a.1.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (A, Bb)', 'data': 'a.2.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (A, testoption)', 'data': 'a.3.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (A)', 'data': 'a._total.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (B, Aa)', 'data': 'b.1.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (B, Bb)', 'data': 'b.2.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (B, testoption)', 'data': 'b.3.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'title': 'Two (B)', 'data': 'b._total.two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}}]},
            result)

    def test_rollup_cont_cat_cat_dims_multi_metric_df(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        result = self.dt_tx.transform(mock_df.rollup_cont_cat_cat_dims_multi_metric_df,
                                      mock_df.rollup_cont_cat_cat_dims_multi_metric_schema)
        self.assertDictEqual({
            'columns': [{'data': 'cont', 'title': 'Cont',
                         'render': {'_': 'value', 'type': 'value', 'sort': 'value'}},
                        {'data': '_total._total.one', 'title': 'One',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a._total.one', 'title': 'One (A)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.y.one', 'title': 'One (A, Y)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.z.one', 'title': 'One (A, Z)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b._total.one', 'title': 'One (B)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.y.one', 'title': 'One (B, Y)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.z.one', 'title': 'One (B, Z)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': '_total._total.two', 'title': 'Two',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a._total.two', 'title': 'Two (A)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.y.two', 'title': 'Two (A, Y)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'a.z.two', 'title': 'Two (A, Z)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b._total.two', 'title': 'Two (B)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.y.two', 'title': 'Two (B, Y)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}},
                        {'data': 'b.z.two', 'title': 'Two (B, Z)',
                         'render': {'_': 'display', 'type': 'value', 'sort': 'value'}}],
            'data': [{'b': {'z': {'two': {'value': 6, 'display': '6'}, 'one': {'value': 3, 'display': '3'}},
                            Totals.key: {'two': {'value': 10, 'display': '10'}, 'one': {'value': 5, 'display': '5'}},
                            'y': {'two': {'value': 4, 'display': '4'}, 'one': {'value': 2, 'display': '2'}}},
                      'a': {'z': {'two': {'value': 2, 'display': '2'}, 'one': {'value': 1, 'display': '1'}},
                            Totals.key: {'two': {'value': 2, 'display': '2'}, 'one': {'value': 1, 'display': '1'}},
                            'y': {'two': {'value': 0, 'display': '0'}, 'one': {'value': 0, 'display': '0'}}},
                      'cont': {'value': 0},
                      Totals.key: {
                          Totals.key: {'two': {'value': 24, 'display': '24'}, 'one': {'value': 12, 'display': '12'}}}},
                     {'b': {'z': {'two': {'value': 14, 'display': '14'}, 'one': {'value': 7, 'display': '7'}},
                            Totals.key: {'two': {'value': 26, 'display': '26'}, 'one': {'value': 13, 'display': '13'}},
                            'y': {'two': {'value': 12, 'display': '12'}, 'one': {'value': 6, 'display': '6'}}},
                      'a': {'z': {'two': {'value': 10, 'display': '10'}, 'one': {'value': 5, 'display': '5'}},
                            Totals.key: {'two': {'value': 18, 'display': '18'}, 'one': {'value': 9, 'display': '9'}},
                            'y': {'two': {'value': 8, 'display': '8'}, 'one': {'value': 4, 'display': '4'}}},
                      'cont': {'value': 1},
                      Totals.key: {
                          Totals.key: {'two': {'value': 88, 'display': '88'}, 'one': {'value': 44, 'display': '44'}}}},
                     {'b': {'z': {'two': {'value': 22, 'display': '22'}, 'one': {'value': 11, 'display': '11'}},
                            Totals.key: {'two': {'value': 42, 'display': '42'}, 'one': {'value': 21, 'display': '21'}},
                            'y': {'two': {'value': 20, 'display': '20'}, 'one': {'value': 10, 'display': '10'}}},
                      'a': {'z': {'two': {'value': 18, 'display': '18'}, 'one': {'value': 9, 'display': '9'}},
                            Totals.key: {'two': {'value': 34, 'display': '34'}, 'one': {'value': 17, 'display': '17'}},
                            'y': {'two': {'value': 16, 'display': '16'}, 'one': {'value': 8, 'display': '8'}}},
                      'cont': {'value': 2},
                      Totals.key: {Totals.key: {'two': {'value': 152, 'display': '152'},
                                                'one': {'value': 76, 'display': '76'}}}},
                     {'b': {'z': {'two': {'value': 30, 'display': '30'}, 'one': {'value': 15, 'display': '15'}},
                            Totals.key: {'two': {'value': 58, 'display': '58'}, 'one': {'value': 29, 'display': '29'}},
                            'y': {'two': {'value': 28, 'display': '28'}, 'one': {'value': 14, 'display': '14'}}},
                      'a': {'z': {'two': {'value': 26, 'display': '26'}, 'one': {'value': 13, 'display': '13'}},
                            Totals.key: {'two': {'value': 50, 'display': '50'}, 'one': {'value': 25, 'display': '25'}},
                            'y': {'two': {'value': 24, 'display': '24'}, 'one': {'value': 12, 'display': '12'}}},
                      'cont': {'value': 3},
                      Totals.key: {Totals.key: {'two': {'value': 216, 'display': '216'},
                                                'one': {'value': 108, 'display': '108'}}}},
                     {'b': {'z': {'two': {'value': 38, 'display': '38'}, 'one': {'value': 19, 'display': '19'}},
                            Totals.key: {'two': {'value': 74, 'display': '74'}, 'one': {'value': 37, 'display': '37'}},
                            'y': {'two': {'value': 36, 'display': '36'}, 'one': {'value': 18, 'display': '18'}}},
                      'a': {'z': {'two': {'value': 34, 'display': '34'}, 'one': {'value': 17, 'display': '17'}},
                            Totals.key: {'two': {'value': 66, 'display': '66'}, 'one': {'value': 33, 'display': '33'}},
                            'y': {'two': {'value': 32, 'display': '32'}, 'one': {'value': 16, 'display': '16'}}},
                      'cont': {'value': 4},
                      Totals.key: {Totals.key: {'two': {'value': 280, 'display': '280'},
                                                'one': {'value': 140, 'display': '140'}}}},
                     {'b': {'z': {'two': {'value': 46, 'display': '46'}, 'one': {'value': 23, 'display': '23'}},
                            Totals.key: {'two': {'value': 90, 'display': '90'}, 'one': {'value': 45, 'display': '45'}},
                            'y': {'two': {'value': 44, 'display': '44'}, 'one': {'value': 22, 'display': '22'}}},
                      'a': {'z': {'two': {'value': 42, 'display': '42'}, 'one': {'value': 21, 'display': '21'}},
                            Totals.key: {'two': {'value': 82, 'display': '82'}, 'one': {'value': 41, 'display': '41'}},
                            'y': {'two': {'value': 40, 'display': '40'}, 'one': {'value': 20, 'display': '20'}}},
                      'cont': {'value': 5},
                      Totals.key: {Totals.key: {'two': {'value': 344, 'display': '344'},
                                                'one': {'value': 172, 'display': '172'}}}},
                     {'b': {'z': {'two': {'value': 54, 'display': '54'}, 'one': {'value': 27, 'display': '27'}},
                            Totals.key: {'two': {'value': 106, 'display': '106'},
                                         'one': {'value': 53, 'display': '53'}},
                            'y': {'two': {'value': 52, 'display': '52'}, 'one': {'value': 26, 'display': '26'}}},
                      'a': {'z': {'two': {'value': 50, 'display': '50'}, 'one': {'value': 25, 'display': '25'}},
                            Totals.key: {'two': {'value': 98, 'display': '98'}, 'one': {'value': 49, 'display': '49'}},
                            'y': {'two': {'value': 48, 'display': '48'}, 'one': {'value': 24, 'display': '24'}}},
                      'cont': {'value': 6},
                      Totals.key: {Totals.key: {'two': {'value': 408, 'display': '408'},
                                                'one': {'value': 204, 'display': '204'}}}},
                     {'b': {'z': {'two': {'value': 62, 'display': '62'}, 'one': {'value': 31, 'display': '31'}},
                            Totals.key: {'two': {'value': 122, 'display': '122'},
                                         'one': {'value': 61, 'display': '61'}},
                            'y': {'two': {'value': 60, 'display': '60'}, 'one': {'value': 30, 'display': '30'}}},
                      'a': {'z': {'two': {'value': 58, 'display': '58'}, 'one': {'value': 29, 'display': '29'}},
                            Totals.key: {'two': {'value': 114, 'display': '114'},
                                         'one': {'value': 57, 'display': '57'}},
                            'y': {'two': {'value': 56, 'display': '56'}, 'one': {'value': 28, 'display': '28'}}},
                      'cont': {'value': 7},
                      Totals.key: {Totals.key: {'two': {'value': 472, 'display': '472'},
                                                'one': {'value': 236, 'display': '236'}}}}
                     ]},
            result)

    def test_max_cols(self, ):
        settings.datatables_maxcols = 24

        df = mock_df.cont_cat_cat_dims_multi_metric_df.reorder_levels([1, 2, 0])
        schema = mock_df.cont_cat_cat_dims_multi_metric_schema.copy()
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
        # np.nan is converted to None
        result = datatables._safe(np.nan)
        self.assertIsNone(result)

    def test_str_data_point(self):
        result = datatables._safe(u'abc')
        self.assertEqual('abc', result)

    def test_int64_data_point(self):
        # Needs to be cast to python int
        result = datatables._safe(np.int64(1))
        self.assertEqual(int(1), result)

    def test_date_data_point(self):
        # Needs to be converted to milliseconds
        result = datatables._safe(pd.Timestamp(date(2000, 1, 1)))
        self.assertEqual('2000-01-01', result)

    def test_datetime_data_point(self):
        # Needs to be converted to milliseconds
        result = datatables._safe(pd.Timestamp(datetime(2000, 1, 1, 1)))
        self.assertEqual('2000-01-01T01:00:00', result)

    def test_no_precision_with_zero(self):
        result = datatables._pretty(0.0, {})
        self.assertEqual('0', result)

    def test_no_precision_with_non_zero(self):
        result = datatables._pretty(0.01, {})
        self.assertEqual('0.01', result)

    def test_precision(self):
        result = datatables._pretty(0.123456789, {'precision': 2})
        self.assertEqual('0.12', result)

    def test_zero_precision(self):
        result = datatables._pretty(1784.0, {'precision': 0})
        self.assertEqual('1784', result)

    def test_prefix(self):
        result = datatables._pretty(0.12, {'prefix': '$'})
        self.assertEqual('$0.12', result)

    def test_suffix(self):
        result = datatables._pretty(0.12, {'suffix': '€'})
        self.assertEqual('0.12€', result)

    def test_format_value_does_not_prettify_none_string(self):
        value = datatables._format_value('None', {})
        self.assertDictEqual(value, {'value': 'None', 'display': 'None'})

    def test_format_value_does_prettify_non_none_strings(self):
        value = datatables._format_value('abcde', {})
        self.assertDictEqual(value, {'value': 'abcde', 'display': 'abcde'})

    def test_format_value_does_prettify_int_values(self):
        value = datatables._format_value(123, {})
        self.assertDictEqual(value, {'value': 123, 'display': '123'})

    def test_format_value_does_prettify_pandas_date_objects(self):
        value = datatables._format_value(pd.Timestamp(date(2016, 5, 10)), {})
        self.assertDictEqual(value, {'value': '2016-05-10', 'display': '2016-05-10'})
