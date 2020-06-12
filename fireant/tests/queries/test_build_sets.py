from unittest import TestCase

from pypika import (
    Table,
    functions as fn,
)

import fireant as f
from fireant.tests.dataset.mocks import test_database

test_table = Table("test")
ds = f.DataSet(
    table=test_table,
    database=test_database,
    fields=[
        f.Field("date", definition=test_table.date, data_type=f.DataType.date),
        f.Field("text", definition=test_table.text, data_type=f.DataType.text),
        f.Field("number", definition=test_table.number, data_type=f.DataType.number),
        f.Field("boolean", definition=test_table.boolean, data_type=f.DataType.boolean),
        f.Field(
            "aggr_number",
            definition=fn.Sum(test_table.number),
            data_type=f.DataType.number,
        ),
    ],
)

# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class ResultSetTests(TestCase):
    def test_dimension_is_replaced_by_default_when_result_set_filter_is_present(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(f.ResultSet(ds.fields.text == "abc"))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$set(text='abc')\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$set(text='abc')\" "
            "ORDER BY \"$set(text='abc')\"",
            str(queries[0]),
        )

    def test_dimension_is_replaced_by_default_in_the_target_dimension_place_when_result_set_filter_is_present(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.date)
            .dimension(ds.fields.text)
            .dimension(ds.fields.boolean)
            .filter(f.ResultSet(ds.fields.text == "abc"))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"date" "$date",'
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$set(text='abc')\","
            '"boolean" "$boolean",'
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'GROUP BY "$date","$set(text=\'abc\')","$boolean" '
            'ORDER BY "$date","$set(text=\'abc\')","$boolean"',
            str(queries[0]),
        )

    def test_dimension_is_inserted_before_conditional_dimension_when_result_set_filter_wont_ignore_dimensions(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(f.ResultSet(ds.fields.text == "abc", will_ignore_dimensions=False))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$set(text='abc')\","
            '"text" "$text",'
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'GROUP BY "$set(text=\'abc\')","$text" '
            'ORDER BY "$set(text=\'abc\')","$text"',
            str(queries[0]),
        )

    def test_dimension_is_inserted_in_dimensions_even_when_not_selected(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .filter(f.ResultSet(ds.fields.text == "abc"))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$set(text='abc')\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$set(text='abc')\" "
            "ORDER BY \"$set(text='abc')\"",
            str(queries[0]),
        )

    def test_dimension_is_inserted_as_last_dimension_when_not_selected(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.date)
            .dimension(ds.fields.boolean)
            .filter(f.ResultSet(ds.fields.text == "abc"))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"date" "$date",'
            '"boolean" "$boolean",'
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$set(text='abc')\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'GROUP BY "$date","$boolean","$set(text=\'abc\')" '
            'ORDER BY "$date","$boolean","$set(text=\'abc\')"',
            str(queries[0]),
        )

    def test_dimension_uses_set_label_kwarg_and_None_for_complement(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(f.ResultSet(ds.fields.text == "abc", set_label="Text is ABC"))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'Text is ABC' ELSE NULL END "
            "\"$set(text='abc')\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$set(text='abc')\" "
            "ORDER BY \"$set(text='abc')\"",
            str(queries[0]),
        )

    def test_dimension_uses_complement_label_kwarg_and_None_for_set(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(
                f.ResultSet(ds.fields.text == "abc", complement_label="Text is NOT ABC")
            )
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN NULL ELSE 'Text is NOT ABC' END "
            "\"$set(text='abc')\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$set(text='abc')\" "
            "ORDER BY \"$set(text='abc')\"",
            str(queries[0]),
        )

    def test_dimension_uses_both_set_and_complement_label_kwargs_when_available(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(
                f.ResultSet(
                    ds.fields.text == "abc",
                    set_label="Text is ABC",
                    complement_label="Text is NOT ABC",
                )
            )
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'Text is ABC' ELSE 'Text is NOT ABC' END "
            "\"$set(text='abc')\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$set(text='abc')\" "
            "ORDER BY \"$set(text='abc')\"",
            str(queries[0]),
        )
