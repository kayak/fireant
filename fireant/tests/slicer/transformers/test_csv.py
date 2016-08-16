# coding: utf-8
from fireant.slicer.transformers import CSVRowIndexTransformer, CSVColumnIndexTransformer
from fireant.tests.slicer.transformers.base import BaseTransformerTests


class CSVRowIndexTransformerTests(BaseTransformerTests):
    maxDiff = None
    csv_tx = CSVRowIndexTransformer()

    def test_no_dims_single_metric(self):
        df = self.no_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.no_dims_multi_metric_schema)

        self.assertEqual('One,Two,Three,Four,Five,Six,Seven,Eight\n'
                         '0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7\n', result)

    def test_cont_dim_single_metric(self):
        df = self.cont_dim_single_metric_df

        result = self.csv_tx.transform(df, self.cont_dim_single_metric_schema)

        self.assertEqual('Cont,One\n'
                         '0,0.0\n'
                         '1,0.1\n'
                         '2,0.2\n'
                         '3,0.3\n'
                         '4,0.4\n'
                         '5,0.5\n'
                         '6,0.6\n'
                         '7,0.7\n', result)

    def test_cont_dim_multi_metric(self):
        df = self.cont_dim_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_dim_multi_metric_schema)

        self.assertEqual('Cont,One,Two\n'
                         '0,0.0,1.0\n'
                         '1,0.1,1.1\n'
                         '2,0.2,1.2\n'
                         '3,0.3,1.3\n'
                         '4,0.4,1.4\n'
                         '5,0.5,1.5\n'
                         '6,0.6,1.6\n'
                         '7,0.7,1.7\n', result)

    def test_cont_cat_dim_single_metric(self):
        df = self.cont_cat_dims_single_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_single_metric_schema)

        self.assertEqual('Cont,Cat1,One\n'
                         '0,A,1\n'
                         '0,B,2\n'
                         '1,A,3\n'
                         '1,B,4\n'
                         '2,A,5\n'
                         '2,B,6\n'
                         '3,A,7\n'
                         '3,B,8\n'
                         '4,A,9\n'
                         '4,B,10\n'
                         '5,A,11\n'
                         '5,B,12\n'
                         '6,A,13\n'
                         '6,B,14\n'
                         '7,A,15\n'
                         '7,B,16\n', result)

    def test_cont_cat_dim_multi_metric(self):
        df = self.cont_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,Cat1,One,Two\n'
                         '0,A,1,10\n'
                         '0,B,2,20\n'
                         '1,A,3,30\n'
                         '1,B,4,40\n'
                         '2,A,5,50\n'
                         '2,B,6,60\n'
                         '3,A,7,70\n'
                         '3,B,8,80\n'
                         '4,A,9,90\n'
                         '4,B,10,100\n'
                         '5,A,11,110\n'
                         '5,B,12,120\n'
                         '6,A,13,130\n'
                         '6,B,14,140\n'
                         '7,A,15,150\n'
                         '7,B,16,160\n', result)

    def test_cont_cat_cat_dim_multi_metric(self):
        df = self.cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,Cat1,Cat2,One,Two\n'
                         '0,A,Y,1,10\n'
                         '0,A,Z,2,20\n'
                         '0,B,Y,3,30\n'
                         '0,B,Z,4,40\n'
                         '1,A,Y,5,50\n'
                         '1,A,Z,6,60\n'
                         '1,B,Y,7,70\n'
                         '1,B,Z,8,80\n'
                         '2,A,Y,9,90\n'
                         '2,A,Z,10,100\n'
                         '2,B,Y,11,110\n'
                         '2,B,Z,12,120\n'
                         '3,A,Y,13,130\n'
                         '3,A,Z,14,140\n'
                         '3,B,Y,15,150\n'
                         '3,B,Z,16,160\n'
                         '4,A,Y,17,170\n'
                         '4,A,Z,18,180\n'
                         '4,B,Y,19,190\n'
                         '4,B,Z,20,200\n'
                         '5,A,Y,21,210\n'
                         '5,A,Z,22,220\n'
                         '5,B,Y,23,230\n'
                         '5,B,Z,24,240\n'
                         '6,A,Y,25,250\n'
                         '6,A,Z,26,260\n'
                         '6,B,Y,27,270\n'
                         '6,B,Z,28,280\n'
                         '7,A,Y,29,290\n'
                         '7,A,Z,30,300\n'
                         '7,B,Y,31,310\n'
                         '7,B,Z,32,320\n', result)

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,Cat1,Cat2,One,Two\n'
                         '0,,,20,200\n'
                         '0,A,,3,30\n'
                         '0,A,Y,1,10\n'
                         '0,A,Z,2,20\n'
                         '0,B,,7,70\n'
                         '0,B,Y,3,30\n'
                         '0,B,Z,4,40\n'
                         '1,,,52,520\n'
                         '1,A,,11,110\n'
                         '1,A,Y,5,50\n'
                         '1,A,Z,6,60\n'
                         '1,B,,15,150\n'
                         '1,B,Y,7,70\n'
                         '1,B,Z,8,80\n'
                         '2,,,84,840\n'
                         '2,A,,19,190\n'
                         '2,A,Y,9,90\n'
                         '2,A,Z,10,100\n'
                         '2,B,,23,230\n'
                         '2,B,Y,11,110\n'
                         '2,B,Z,12,120\n'
                         '3,,,116,1160\n'
                         '3,A,,27,270\n'
                         '3,A,Y,13,130\n'
                         '3,A,Z,14,140\n'
                         '3,B,,31,310\n'
                         '3,B,Y,15,150\n'
                         '3,B,Z,16,160\n'
                         '4,,,148,1480\n'
                         '4,A,,35,350\n'
                         '4,A,Y,17,170\n'
                         '4,A,Z,18,180\n'
                         '4,B,,39,390\n'
                         '4,B,Y,19,190\n'
                         '4,B,Z,20,200\n'
                         '5,,,180,1800\n'
                         '5,A,,43,430\n'
                         '5,A,Y,21,210\n'
                         '5,A,Z,22,220\n'
                         '5,B,,47,470\n'
                         '5,B,Y,23,230\n'
                         '5,B,Z,24,240\n'
                         '6,,,212,2120\n'
                         '6,A,,51,510\n'
                         '6,A,Y,25,250\n'
                         '6,A,Z,26,260\n'
                         '6,B,,55,550\n'
                         '6,B,Y,27,270\n'
                         '6,B,Z,28,280\n'
                         '7,,,244,2440\n'
                         '7,A,,59,590\n'
                         '7,A,Y,29,290\n'
                         '7,A,Z,30,300\n'
                         '7,B,,63,630\n'
                         '7,B,Y,31,310\n'
                         '7,B,Z,32,320\n', result)


class CSVColumnIndexTransformerTests(CSVRowIndexTransformerTests):
    maxDiff = None
    csv_tx = CSVColumnIndexTransformer()

    def test_cont_cat_dim_single_metric(self):
        df = self.cont_cat_dims_single_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_single_metric_schema)

        self.assertEqual('Cont,One (A),One (B)\n'
                         '0,1,2\n'
                         '1,3,4\n'
                         '2,5,6\n'
                         '3,7,8\n'
                         '4,9,10\n'
                         '5,11,12\n'
                         '6,13,14\n'
                         '7,15,16\n', result)

    def test_cont_cat_dim_multi_metric(self):
        df = self.cont_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,One (A),One (B),Two (A),Two (B)\n'
                         '0,1,2,10,20\n'
                         '1,3,4,30,40\n'
                         '2,5,6,50,60\n'
                         '3,7,8,70,80\n'
                         '4,9,10,90,100\n'
                         '5,11,12,110,120\n'
                         '6,13,14,130,140\n'
                         '7,15,16,150,160\n', result)

    def test_cont_cat_cat_dim_multi_metric(self):
        df = self.cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.cont_cat_cat_dims_multi_metric_schema)

        self.assertEqual('Cont,"One (A, Y)","One (A, Z)","One (B, Y)","One (B, Z)",'  # continues on next line
                         '"Two (A, Y)","Two (A, Z)","Two (B, Y)","Two (B, Z)"\n'
                         '0,1,2,3,4,10,20,30,40\n'
                         '1,5,6,7,8,50,60,70,80\n'
                         '2,9,10,11,12,90,100,110,120\n'
                         '3,13,14,15,16,130,140,150,160\n'
                         '4,17,18,19,20,170,180,190,200\n'
                         '5,21,22,23,24,210,220,230,240\n'
                         '6,25,26,27,28,250,260,270,280\n'
                         '7,29,30,31,32,290,300,310,320\n', result)

    def test_rollup_cont_cat_cat_dim_multi_metric(self):
        df = self.rollup_cont_cat_cat_dims_multi_metric_df

        result = self.csv_tx.transform(df, self.rollup_cont_cat_cat_dims_multi_metric_schema)

        self.assertEqual(
            'Cont,One,One (A),"One (A, Y)","One (A, Z)",One (B),"One (B, Y)","One (B, Z)",'  # continues on next line
            'Two,Two (A),"Two (A, Y)","Two (A, Z)",Two (B),"Two (B, Y)","Two (B, Z)"\n'
            '0,20,3,1,2,7,3,4,200,30,10,20,70,30,40\n'
            '1,52,11,5,6,15,7,8,520,110,50,60,150,70,80\n'
            '2,84,19,9,10,23,11,12,840,190,90,100,230,110,120\n'
            '3,116,27,13,14,31,15,16,1160,270,130,140,310,150,160\n'
            '4,148,35,17,18,39,19,20,1480,350,170,180,390,190,200\n'
            '5,180,43,21,22,47,23,24,1800,430,210,220,470,230,240\n'
            '6,212,51,25,26,55,27,28,2120,510,250,260,550,270,280\n'
            '7,244,59,29,30,63,31,32,2440,590,290,300,630,310,320\n',
            result
        )
