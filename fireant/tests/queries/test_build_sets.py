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
    maxDiff = None

    def test_no_metric_is_removed_when_result_set_metric_filter_is_present(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(f.ResultSet(ds.fields.aggr_number > 10)).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN SUM(\"number\")>10 THEN 'set(SUM(number)>10)' "
            "ELSE 'complement(SUM(number)>10)' END \"$set(SUM(number)>10)\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

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
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
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
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$text\","
            '"boolean" "$boolean",'
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'GROUP BY "$date","$text","$boolean" '
            'ORDER BY "$date","$text","$boolean" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_with_dimension_modifier_is_replaced_by_default_when_result_set_filter_is_present(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.date)
            .dimension(f.Rollup(ds.fields.boolean))
            .filter(f.ResultSet(ds.fields.boolean == True))
            .sql
        )

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is the same as without totals'):
            self.assertEqual(
                "SELECT "
                '"date" "$date",'
                "CASE WHEN \"boolean\"=true THEN 'set(boolean=true)' ELSE 'complement(boolean=true)' END \"$boolean\","
                'SUM("number") "$aggr_number" '
                'FROM "test" '
                'GROUP BY "$date","$boolean" '
                'ORDER BY "$date","$boolean" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest('totals dimension is replaced with _FIREANT_ROLLUP_VALUE_'):
            self.assertEqual(
                "SELECT "
                '"date" "$date",'
                '\'_FIREANT_ROLLUP_VALUE_\' "$boolean",'
                'SUM("number") "$aggr_number" '
                'FROM "test" '
                'GROUP BY "$date" '
                'ORDER BY "$date","$boolean" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_dimension_is_inserted_before_conditional_dimension_when_result_set_filter_wont_ignore_dimensions(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(f.ResultSet(ds.fields.text == "abc", will_replace_referenced_dimension=False))
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
            'ORDER BY "$set(text=\'abc\')","$text" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_breaks_complement_down_when_result_set_filter_wont_group_complement(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(f.ResultSet(ds.fields.text == "abc", will_group_complement=False))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE \"text\" END \"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_dimension_is_inserted_in_dimensions_even_when_not_selected(self):
        queries = ds.query.widget(f.Pandas(ds.fields.aggr_number)).filter(f.ResultSet(ds.fields.text == "abc")).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
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
            "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            'GROUP BY "$date","$boolean","$text" '
            'ORDER BY "$date","$boolean","$text" '
            'LIMIT 200000',
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
            "CASE WHEN \"text\"='abc' THEN 'Text is ABC' ELSE null END "
            "\"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_dimension_breaks_complement_down_even_when_set_label_is_set_when_result_set_filter_wont_group_complement(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(
                f.ResultSet(
                    ds.fields.text == "abc",
                    set_label="IS ABC",
                    will_group_complement=False,
                )
            )
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'IS ABC' ELSE \"text\" END \"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_dimension_breaks_complement_down_even_when_both_labels_are_set_but_wont_group_complement(
        self,
    ):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(
                f.ResultSet(
                    ds.fields.text == "abc",
                    set_label="IS ABC",
                    complement_label="OTHERS",
                    will_group_complement=False,
                )
            )
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN 'IS ABC' ELSE \"text\" END \"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_dimension_uses_complement_label_kwarg_and_None_for_set(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.text)
            .filter(f.ResultSet(ds.fields.text == "abc", complement_label="Text is NOT ABC"))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "CASE WHEN \"text\"='abc' THEN null ELSE 'Text is NOT ABC' END "
            "\"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
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
            "\"$text\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            "GROUP BY \"$text\" "
            "ORDER BY \"$text\" "
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_dimension_is_replaced_when_references_are_present(self):
        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields.date)
            .dimension(ds.fields.boolean)
            .reference(f.WeekOverWeek(ds.fields.date))
            .filter(f.ResultSet(ds.fields.text == "abc"))
            .sql
        )

        self.assertEqual(len(queries), 2)

        with self.subTest("base query"):
            self.assertEqual(
                "SELECT "
                '"date" "$date",'
                '"boolean" "$boolean",'
                "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$text\","
                'SUM("number") "$aggr_number" '
                'FROM "test" '
                'GROUP BY "$date","$boolean","$text" '
                'ORDER BY "$date","$boolean","$text" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("ref query"):
            self.assertEqual(
                "SELECT "
                'TIMESTAMPADD(week,1,"date") "$date",'
                '"boolean" "$boolean",'
                "CASE WHEN \"text\"='abc' THEN 'set(text=''abc'')' ELSE 'complement(text=''abc'')' END \"$text\","
                'SUM("number") "$aggr_number_wow" '
                'FROM "test" '
                'GROUP BY "$date","$boolean","$text" '
                'ORDER BY "$date","$boolean","$text" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_dimension_filter_variations_with_sets(self):
        for field_alias, fltr in [
            ('text', ds.fields.text.like("%abc%")),
            ('text', ds.fields.text.not_like("%abc%")),
            ('text', ds.fields.text.like("%abc%", "%cde%")),
            ('text', ds.fields.text.not_like("%abc%", "%cde%")),
            ('text', ds.fields.text.isin(["abc"])),
            ('text', ds.fields.text.notin(["abc"])),
            ('date', ds.fields.date.between('date1', 'date2')),
            ('number', ds.fields.number.between(5, 15)),
            ('number', ds.fields.number.isin([1, 2, 3])),
            ('number', ds.fields.number.notin([1, 2, 3])),
        ]:
            fltr_sql = fltr.definition.get_sql(quote_char="")

            with self.subTest(fltr_sql):
                queries = (
                    ds.query.widget(f.Pandas(ds.fields.aggr_number))
                    .dimension(ds.fields[field_alias])
                    .filter(f.ResultSet(fltr, set_label='set_A', complement_label='set_B'))
                    .sql
                )

                self.assertEqual(len(queries), 1)
                self.assertEqual(
                    "SELECT "
                    f"CASE WHEN {fltr} THEN 'set_A' ELSE 'set_B' END \"${field_alias}\","
                    'SUM("number") "$aggr_number" '
                    'FROM "test" '
                    f"GROUP BY \"${field_alias}\" "
                    f"ORDER BY \"${field_alias}\" "
                    "LIMIT 200000",
                    str(queries[0]),
                )

    def test_deeply_nested_dimension_filter_with_sets(self):
        field_alias = 'text'
        fltr = ds.fields.text.like(
            fn.Concat(
                fn.Upper(fn.Trim(fn.Concat('%ab', ds.fields.number))),
                ds.fields.aggr_number,
                fn.Concat(ds.fields.date.between('date1', 'date2'), 'c%'),
            )
        )

        queries = (
            ds.query.widget(f.Pandas(ds.fields.aggr_number))
            .dimension(ds.fields[field_alias])
            .filter(f.ResultSet(fltr, set_label='set_A', complement_label='set_B'))
            .sql
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            f"CASE WHEN {fltr} THEN 'set_A' ELSE 'set_B' END \"${field_alias}\","
            'SUM("number") "$aggr_number" '
            'FROM "test" '
            f"GROUP BY \"${field_alias}\" "
            f"ORDER BY \"${field_alias}\" "
            "LIMIT 200000",
            str(queries[0]),
        )
