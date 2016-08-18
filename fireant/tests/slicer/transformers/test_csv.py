# coding: utf-8
from fireant.slicer.transformers import CSVRowIndexTransformer, CSVColumnIndexTransformer
from fireant.tests.slicer.transformers.base import BaseTransformerTests


class CSVRowIndexTransformerTests(BaseTransformerTests):
    csv_tx = CSVRowIndexTransformer()

    def test_no_dims_single_metric(self):
        df = self.no_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.no_dims_multi_metric_schema)

        self.assertEqual('One,Two,Three,Four,Five,Six,Seven,Eight\n'
                         '0,1,2,3,4,5,6,7\n', result)

    def test_cont_dim_single_metric(self):
        df = self.cont_dim_single_metric_df

        result = self.csv_tx.transform(df, self.cont_dim_single_metric_schema)

        self.assertEqual('Cont,One\n'
                         '0,0\n'
                         '1,1\n'
                         '2,2\n'
                         '3,3\n'
                         '4,4\n'
                         '5,5\n'
                         '6,6\n'
                         '7,7\n', result)

    def test_cont_dim_multi_metric(self):
        df = self.cont_dim_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_dim_multi_metric_schema)

        self.assertEqual('Cont,One,Two\n'
                         '0,0,0\n'
                         '1,1,2\n'
                         '2,2,4\n'
                         '3,3,6\n'
                         '4,4,8\n'
                         '5,5,10\n'
                         '6,6,12\n'
                         '7,7,14\n', result)

    def test_cont_cat_dim_single_metric(self):
        df = self.cont_cat_dims_single_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_single_metric_schema)

        self.assertEqual('Cont,Cat1,One\n'
                         '0,A,0\n'
                         '0,B,1\n'
                         '1,A,2\n'
                         '1,B,3\n'
                         '2,A,4\n'
                         '2,B,5\n'
                         '3,A,6\n'
                         '3,B,7\n'
                         '4,A,8\n'
                         '4,B,9\n'
                         '5,A,10\n'
                         '5,B,11\n'
                         '6,A,12\n'
                         '6,B,13\n'
                         '7,A,14\n'
                         '7,B,15\n', result)

    def test_cont_cat_dim_multi_metric(self):
        df = self.cont_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,Cat1,One,Two\n'
                         '0,A,0,0\n'
                         '0,B,1,2\n'
                         '1,A,2,4\n'
                         '1,B,3,6\n'
                         '2,A,4,8\n'
                         '2,B,5,10\n'
                         '3,A,6,12\n'
                         '3,B,7,14\n'
                         '4,A,8,16\n'
                         '4,B,9,18\n'
                         '5,A,10,20\n'
                         '5,B,11,22\n'
                         '6,A,12,24\n'
                         '6,B,13,26\n'
                         '7,A,14,28\n'
                         '7,B,15,30\n', result)

    def test_cont_cat_cat_dim_multi_metric(self):
        df = self.cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,Cat1,Cat2,One,Two\n'
                         '0,A,Y,0,0\n'
                         '0,A,Z,1,2\n'
                         '0,B,Y,2,4\n'
                         '0,B,Z,3,6\n'
                         '1,A,Y,4,8\n'
                         '1,A,Z,5,10\n'
                         '1,B,Y,6,12\n'
                         '1,B,Z,7,14\n'
                         '2,A,Y,8,16\n'
                         '2,A,Z,9,18\n'
                         '2,B,Y,10,20\n'
                         '2,B,Z,11,22\n'
                         '3,A,Y,12,24\n'
                         '3,A,Z,13,26\n'
                         '3,B,Y,14,28\n'
                         '3,B,Z,15,30\n'
                         '4,A,Y,16,32\n'
                         '4,A,Z,17,34\n'
                         '4,B,Y,18,36\n'
                         '4,B,Z,19,38\n'
                         '5,A,Y,20,40\n'
                         '5,A,Z,21,42\n'
                         '5,B,Y,22,44\n'
                         '5,B,Z,23,46\n'
                         '6,A,Y,24,48\n'
                         '6,A,Z,25,50\n'
                         '6,B,Y,26,52\n'
                         '6,B,Z,27,54\n'
                         '7,A,Y,28,56\n'
                         '7,A,Z,29,58\n'
                         '7,B,Y,30,60\n'
                         '7,B,Z,31,62\n', result)

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,Cat1,Cat2,One,Two\n'
                         '0,,,12,24\n'
                         '0,A,,1,2\n'
                         '0,A,Y,0,0\n'
                         '0,A,Z,1,2\n'
                         '0,B,,5,10\n'
                         '0,B,Y,2,4\n'
                         '0,B,Z,3,6\n'
                         '1,,,44,88\n'
                         '1,A,,9,18\n'
                         '1,A,Y,4,8\n'
                         '1,A,Z,5,10\n'
                         '1,B,,13,26\n'
                         '1,B,Y,6,12\n'
                         '1,B,Z,7,14\n'
                         '2,,,76,152\n'
                         '2,A,,17,34\n'
                         '2,A,Y,8,16\n'
                         '2,A,Z,9,18\n'
                         '2,B,,21,42\n'
                         '2,B,Y,10,20\n'
                         '2,B,Z,11,22\n'
                         '3,,,108,216\n'
                         '3,A,,25,50\n'
                         '3,A,Y,12,24\n'
                         '3,A,Z,13,26\n'
                         '3,B,,29,58\n'
                         '3,B,Y,14,28\n'
                         '3,B,Z,15,30\n'
                         '4,,,140,280\n'
                         '4,A,,33,66\n'
                         '4,A,Y,16,32\n'
                         '4,A,Z,17,34\n'
                         '4,B,,37,74\n'
                         '4,B,Y,18,36\n'
                         '4,B,Z,19,38\n'
                         '5,,,172,344\n'
                         '5,A,,41,82\n'
                         '5,A,Y,20,40\n'
                         '5,A,Z,21,42\n'
                         '5,B,,45,90\n'
                         '5,B,Y,22,44\n'
                         '5,B,Z,23,46\n'
                         '6,,,204,408\n'
                         '6,A,,49,98\n'
                         '6,A,Y,24,48\n'
                         '6,A,Z,25,50\n'
                         '6,B,,53,106\n'
                         '6,B,Y,26,52\n'
                         '6,B,Z,27,54\n'
                         '7,,,236,472\n'
                         '7,A,,57,114\n'
                         '7,A,Y,28,56\n'
                         '7,A,Z,29,58\n'
                         '7,B,,61,122\n'
                         '7,B,Y,30,60\n'
                         '7,B,Z,31,62\n', result)


class CSVColumnIndexTransformerTests(CSVRowIndexTransformerTests):
    csv_tx = CSVColumnIndexTransformer()

    def test_cont_cat_dim_single_metric(self):
        df = self.cont_cat_dims_single_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_single_metric_schema)

        self.assertEqual('Cont,One (A),One (B)\n'
                         '0,0,1\n'
                         '1,2,3\n'
                         '2,4,5\n'
                         '3,6,7\n'
                         '4,8,9\n'
                         '5,10,11\n'
                         '6,12,13\n'
                         '7,14,15\n', result)

    def test_cont_cat_dim_multi_metric(self):
        df = self.cont_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_multi_metric_schema)
        print(result)

        self.assertEqual('Cont,One (A),One (B),Two (A),Two (B)\n'
                         '0,0,1,0,2\n'
                         '1,2,3,4,6\n'
                         '2,4,5,8,10\n'
                         '3,6,7,12,14\n'
                         '4,8,9,16,18\n'
                         '5,10,11,20,22\n'
                         '6,12,13,24,26\n'
                         '7,14,15,28,30\n', result)

    def test_cont_cat_cat_dim_multi_metric(self):
        df = self.cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_cat_dims_multi_metric_schema)
        print(result)

        self.assertEqual('Cont,"One (A, Y)","One (A, Z)","One (B, Y)","One (B, Z)",'
                         '"Two (A, Y)","Two (A, Z)","Two (B, Y)","Two (B, Z)"\n'
                         '0,0,1,2,3,0,2,4,6\n'
                         '1,4,5,6,7,8,10,12,14\n'
                         '2,8,9,10,11,16,18,20,22\n'
                         '3,12,13,14,15,24,26,28,30\n'
                         '4,16,17,18,19,32,34,36,38\n'
                         '5,20,21,22,23,40,42,44,46\n'
                         '6,24,25,26,27,48,50,52,54\n'
                         '7,28,29,30,31,56,58,60,62\n', result)

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)
        print(result)

        self.assertEqual('Cont,One,One (A),"One (A, Y)","One (A, Z)",One (B),"One (B, Y)","One (B, Z)",'
                         'Two,Two (A),"Two (A, Y)","Two (A, Z)",Two (B),"Two (B, Y)","Two (B, Z)"\n'
                         '0,12,1,0,1,5,2,3,24,2,0,2,10,4,6\n'
                         '1,44,9,4,5,13,6,7,88,18,8,10,26,12,14\n'
                         '2,76,17,8,9,21,10,11,152,34,16,18,42,20,22\n'
                         '3,108,25,12,13,29,14,15,216,50,24,26,58,28,30\n'
                         '4,140,33,16,17,37,18,19,280,66,32,34,74,36,38\n'
                         '5,172,41,20,21,45,22,23,344,82,40,42,90,44,46\n'
                         '6,204,49,24,25,53,26,27,408,98,48,50,106,52,54\n'
                         '7,236,57,28,29,61,30,31,472,114,56,58,122,60,62\n', result)
