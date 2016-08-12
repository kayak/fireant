# coding: utf-8
from datetime import date
from unittest import TestCase

import numpy as np
import pandas as pd

from fireant.slicer.transformers import TableIndex, DataTablesTransformer
from fireant.slicer.transformers import datatables
from fireant.tests.slicer.transformers.base import BaseTransformerTests


class DataTablesRowIndexTransformerTests(BaseTransformerTests):
    dt_tx = DataTablesTransformer(TableIndex.row_index)

    def _evaluate_table(self, result, num_rows=1):
        self.assertSetEqual({'draw', 'recordsTotal', 'recordsFiltered', 'data'}, set(result.keys()))

        self.assertEqual(num_rows, result['recordsTotal'])
        self.assertEqual(num_rows, result['recordsFiltered'])
        self.assertEqual(num_rows, len(result['data']))

    def test_no_dims_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        df = self.no_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.no_dims_multi_metric_schema)

        self.assertSetEqual({'draw', 'recordsTotal', 'recordsFiltered', 'data'}, set(result.keys()))

        self.assertEqual(1, result['recordsTotal'])
        self.assertEqual(1, result['recordsFiltered'])
        self.assertEqual(1, len(result['data']))

        d = result['data'][0]

        self.assertSetEqual({'DT_RowId',
                             'One', 'Two', 'Three', 'Four',
                             'Five', 'Six', 'Seven', 'Eight'}, set(d.keys()))
        self.assertEqual('row_0', d['DT_RowId'])
        self.assertEqual(0., d['One'])
        self.assertEqual(.1, d['Two'])
        self.assertEqual(.2, d['Three'])
        self.assertEqual(.3, d['Four'])
        self.assertEqual(.4, d['Five'])
        self.assertEqual(.5, d['Six'])
        self.assertEqual(.6, d['Seven'])
        self.assertEqual(.7, d['Eight'])

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        df = self.cont_dim_single_metric_df

        result = self.dt_tx.transform(df, self.cont_dim_single_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'One'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            self.assertEqual(row['one'], data['One'])

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        df = self.cont_dim_multi_metric_df

        result = self.dt_tx.transform(df, self.cont_dim_multi_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'One', 'Two'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            self.assertEqual(row['one'], data['One'])
            self.assertEqual(row['two'], data['Two'])

    def test_time_series_date_to_millis(self):
        # Tests transformation of a single-metric, single-dimension result
        df = self.time_dim_single_metric_df

        result = self.dt_tx.transform(df, self.time_dim_single_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (dt, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Date', 'One'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(dt.isoformat(), data['Date'])
            self.assertEqual(row['one'], data['One'])

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        df = self.time_dim_single_metric_ref_df

        result = self.dt_tx.transform(df, self.time_dim_single_metric_ref_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (dt, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Date', 'One', 'One WoW'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(dt.isoformat(), data['Date'])
            self.assertEqual(row[('', 'one')], data['One'])
            self.assertEqual(row[('wow', 'one')], data['One WoW'])

    def test_uni_dim_single_metric(self):
        # Tests transformation of a metric with a unique dimension with one key and label
        df = self.uni_dim_single_metric_df

        result = self.dt_tx.transform(df, self.uni_dim_single_metric_schema)

        self._evaluate_table(result, num_rows=3)

        for i, (data, ((df_label, df_id0), row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Uni1', 'One'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertDictEqual({'uni1_label': df_label, 'uni1_id': df_id0}, data['Uni1'])
            self.assertEqual(row['one'], data['One'])

    def test_uni_dim_multi_metric(self):
        # Tests transformation of two metrics with a unique dimension with two keys and label
        df = self.uni_dim_multi_metric_df

        result = self.dt_tx.transform(df, self.uni_dim_multi_metric_schema)

        self._evaluate_table(result, num_rows=4)

        for i, (data, ((df_label, df_id0, df_id1), row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Uni2', 'One', 'Two'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertDictEqual({'uni2_label': df_label, 'uni2_id0': df_id0, 'uni2_id1': df_id1}, data['Uni2'])
            self.assertEqual(row['one'], data['One'])

    def test_cat_cat_dim_single_metric(self):
        # Tests transformation of a single metric with two categorical dimensions
        df = self.cat_cat_dims_single_metric_df

        result = self.dt_tx.transform(df, self.cat_cat_dims_single_metric_schema)

        self._evaluate_table(result, num_rows=4)

        for i, (data, ((cat1, cat2), row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cat1', 'Cat2', 'One'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(self.shortcuts[cat1], data['Cat1'])
            self.assertEqual(self.shortcuts[cat2], data['Cat2'])
            self.assertEqual(row['one'], data['One'])

    def test_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        df = self.cat_cat_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.cat_cat_dims_multi_metric_schema)

        self._evaluate_table(result, num_rows=4)

        for i, (data, ((cat1, cat2), row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cat1', 'Cat2', 'One', 'Two'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(self.shortcuts[cat1], data['Cat1'])
            self.assertEqual(self.shortcuts[cat2], data['Cat2'])
            self.assertEqual(row['one'], data['One'])
            self.assertEqual(row['two'], data['Two'])

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self._evaluate_table(result, num_rows=56)

        for i, (data, ((cont, cat1, cat2), row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'Cat1', 'Cat2', 'One', 'Two'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            self.assertEqual(self.shortcuts[cat1], data['Cat1'])
            self.assertEqual(self.shortcuts[cat2], data['Cat2'])
            self.assertEqual(row['one'], data['One'])
            self.assertEqual(row['two'], data['Two'])


class DataTablesColumnIndexTransformerTests(BaseTransformerTests):
    dt_tx = DataTablesTransformer(TableIndex.column_index)

    def _evaluate_table(self, result, num_rows=1):
        self.assertSetEqual({'draw', 'recordsTotal', 'recordsFiltered', 'data'}, set(result.keys()))

        self.assertEqual(num_rows, result['recordsTotal'])
        self.assertEqual(num_rows, result['recordsFiltered'])
        self.assertEqual(num_rows, len(result['data']))

    def test_no_dims_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        df = self.no_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.no_dims_multi_metric_schema)

        self._evaluate_table(result)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId',
                                 'One', 'Two', 'Three', 'Four',
                                 'Five', 'Six', 'Seven', 'Eight'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(0., data['One'])
            self.assertEqual(.1, data['Two'])
            self.assertEqual(.2, data['Three'])
            self.assertEqual(.3, data['Four'])
            self.assertEqual(.4, data['Five'])
            self.assertEqual(.5, data['Six'])
            self.assertEqual(.6, data['Seven'])
            self.assertEqual(.7, data['Eight'])

    def test_cont_dim_single_metric(self):
        # Tests transformation of a single metric with a single continuous dimension
        df = self.cont_dim_single_metric_df

        result = self.dt_tx.transform(df, self.cont_dim_single_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'One'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            self.assertEqual(row['one'], data['One'])

    def test_cont_dim_multi_metric(self):
        # Tests transformation of two metrics with a single continuous dimension
        df = self.cont_dim_multi_metric_df

        result = self.dt_tx.transform(df, self.cont_dim_multi_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'One', 'Two'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            self.assertEqual(row['one'], data['One'])
            self.assertEqual(row['two'], data['Two'])

    def test_time_series_date_to_millis(self):
        # Tests transformation of a single-metric, single-dimension result
        df = self.time_dim_single_metric_df

        result = self.dt_tx.transform(df, self.time_dim_single_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (dt, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Date', 'One'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(dt.isoformat(), data['Date'])
            self.assertEqual(row['one'], data['One'])

    def test_time_series_date_with_ref(self):
        # Tests transformation of a single-metric, single-dimension result using a WoW reference
        df = self.time_dim_single_metric_ref_df

        result = self.dt_tx.transform(df, self.time_dim_single_metric_ref_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (dt, row)) in enumerate(zip(result['data'], df.iterrows())):
            self.assertSetEqual({'DT_RowId', 'Date', 'One', 'One WoW'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(dt.isoformat(), data['Date'])
            self.assertEqual(row[('', 'one')], data['One'])
            self.assertEqual(row[('wow', 'one')], data['One WoW'])

    def test_cont_cat_dim_single_metric(self):
        # Tests transformation of a single metric with a continuous and a categorical dimension
        df = self.cont_cat_dims_single_metric_df

        result = self.dt_tx.transform(df, self.cont_cat_dims_single_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.unstack(level=[1]).iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'One (A)', 'One (B)'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            self.assertEqual(row[('one', 'a')], data['One (A)'])
            self.assertEqual(row[('one', 'b')], data['One (B)'])

    def test_cont_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and a categorical dimension
        df = self.cont_cat_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.cont_cat_dims_multi_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.unstack(level=[1]).iterrows())):
            self.assertSetEqual({'DT_RowId', 'Cont', 'One (A)', 'One (B)', 'Two (A)', 'Two (B)'}, set(data.keys()))

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])

            for key0, label0 in [('one', 'One'), ('two', 'Two')]:
                for key1, label1 in [('a', 'A'), ('b', 'B')]:
                    self.assertEqual(
                        row[(key0, key1)],
                        data['%s (%s)' % (label0, label1)]
                    )

    def test_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        df = self.cont_cat_cat_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.cont_cat_cat_dims_multi_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.unstack(level=[1, 2]).iterrows())):
            self.assertSetEqual(
                {
                    'DT_RowId', 'Cont',
                    'One (A, Y)', 'One (A, Z)', 'One (B, Y)', 'One (B, Z)',
                    'Two (A, Y)', 'Two (A, Z)', 'Two (B, Y)', 'Two (B, Z)'
                },
                set(data.keys())
            )

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])

            for key0, label0 in [('one', 'One'), ('two', 'Two')]:
                for key1, label1 in [('a', 'A'), ('b', 'B')]:
                    for key2, label2 in [('y', 'Y'), ('z', 'Z')]:
                        self.assertEqual(
                            row[(key0, key1, key2)],
                            data['%s (%s, %s)' % (label0, label1, label2)]
                        )

    def test_cont_cat_uni_dim_multi_metric(self):
        # Tests transformation of two metrics with a continuous and two categorical dimensions
        df = self.cont_cat_uni_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.cont_cat_uni_dims_multi_metric_schema)

        self._evaluate_table(result, num_rows=8)

        df_row_iter = df.unstack(level=[1, 2, 3, 4]).iterrows()
        for i, (data, (cont, row)) in enumerate(zip(result['data'], df_row_iter)):
            self.assertSetEqual(
                {
                    'DT_RowId', 'Cont',
                    'One (A, Uni2_1)', 'One (A, Uni2_2)', 'One (A, Uni2_3)', 'One (A, Uni2_4)',
                    'One (B, Uni2_1)', 'One (B, Uni2_2)', 'One (B, Uni2_3)', 'One (B, Uni2_4)',
                    'Two (A, Uni2_1)', 'Two (A, Uni2_2)', 'Two (A, Uni2_3)', 'Two (A, Uni2_4)',
                    'Two (B, Uni2_1)', 'Two (B, Uni2_2)', 'Two (B, Uni2_3)', 'Two (B, Uni2_4)',
                },
                set(data.keys())
            )

            self.assertEqual('row_%d' % i, data['DT_RowId'])
            self.assertEqual(cont, data['Cont'])
            for key0, label0 in [('one', 'One'), ('two', 'Two')]:
                for key1, label1 in [('a', 'A'), ('b', 'B')]:
                    for key2a, key2b, key2c, label2 in [('Uni2_1', 1, 100, 'Uni2_1'),
                                                        ('Uni2_2', 2, 200, 'Uni2_2'),
                                                        ('Uni2_3', 3, 300, 'Uni2_3'),
                                                        ('Uni2_4', 4, 400, 'Uni2_4')]:
                        self.assertEqual(
                            row[(key0, key1, key2a, key2b, key2c)],
                            data['%s (%s, %s)' % (label0, label1, label2)]
                        )

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        # Tests transformation of two metrics with two categorical dimensions
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.dt_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self._evaluate_table(result, num_rows=8)

        for i, (data, (cont, row)) in enumerate(zip(result['data'], df.unstack(level=[1, 2]).iterrows())):
            self.assertSetEqual(
                {
                    'DT_RowId', 'Cont',
                    'One', 'One (A)', 'One (A, Y)', 'One (A, Z)', 'One (B)', 'One (B, Y)', 'One (B, Z)',
                    'Two', 'Two (A)', 'Two (A, Y)', 'Two (A, Z)', 'Two (B)', 'Two (B, Y)', 'Two (B, Z)'
                },
                set(data.keys())
            )

            for key0, label0 in [('one', 'One'), ('two', 'Two')]:
                for key1, label1 in [(np.nan, None), ('a', 'A'), ('b', 'B')]:
                    for key2, label2 in [(np.nan, None), ('y', 'Y'), ('z', 'Z')]:
                        dim_labels = ', '.join(filter(None, [label1, label2]))
                        label = '%s (%s)' % (label0, dim_labels) if dim_labels else label0

                        self.assertEqual(
                            row[(key0, key1, key2)],
                            data[label]
                        )

                        # skip the rest of the level if the previous level is rolled up
                        if label1 is None:
                            break


class DatatablesUtilityTests(TestCase):
    def test_nan_data_point(self):
        # Needs to be cast to python int
        result = datatables.format_data_point(np.nan)
        self.assertIsNone(result)

    def test_str_data_point(self):
        result = datatables.format_data_point('abc')
        self.assertEqual('abc', result)

    def test_int64_data_point(self):
        # Needs to be cast to python int
        result = datatables.format_data_point(np.int64(1))
        self.assertEqual(int(1), result)

    def test_datetime_data_point(self):
        # Needs to be converted to milliseconds
        result = datatables.format_data_point(pd.Timestamp(date(2000, 1, 1)))
        self.assertEqual('2000-01-01T00:00:00', result)
