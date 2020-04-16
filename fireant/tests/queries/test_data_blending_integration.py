from unittest import TestCase

import fireant as f
from fireant import (
    DataSet,
    DataType,
    Database,
    Field,
    ReactTable,
)
from pypika import Tables

from fireant.tests.database.mock_database import TestDatabase


class DataSetBlenderIntegrationTests(TestCase):
    maxDiff = None

    def test_use_metric_from_primary_dataset_when_alias_conflicts_with_metric_from_secondary(
        self,
    ):
        db = Database()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric",
                    label="Metric",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric",
                    label="Metric",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = (
            primary_ds.blend(secondary_ds)
            .on_dimensions()
            .extra_fields(
                Field(
                    "metric_share",
                    label="Metric Share",
                    definition=primary_ds.fields.metric / secondary_ds.fields.metric,
                    data_type=DataType.number,
                ),
            )
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp)
            .widget(ReactTable(blend_ds.fields.metric_share))
        ).sql

        (query,) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$metric"/"sq1"."$metric" "$metric_share" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric" '
            'FROM "test0" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(query),
        )

    def test_produce_a_sql_with_multiple_subqueries_in_from_clause_when_blender_not_mapped_on_any_fields(
        self,
    ):
        db = Database()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "metric2",
                    label="Metric2",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on({})

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp)
            .widget(ReactTable(blend_ds.fields.metric1, blend_ds.fields.metric2))
        ).sql

        (query,) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$metric1" "$metric1",'
            '"sq1"."$metric2" "$metric2" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test0" '
            'GROUP BY "$timestamp"'
            ') "sq0",'
            "("
            "SELECT "
            '"metric" "$metric2" '
            'FROM "test1"'
            ') "sq1" '
            'ORDER BY "$timestamp"',
            str(query),
        )

    def test_do_not_include_fields_with_conflicting_aliases_in_subqueries_unless_mapped(
        self,
    ):
        db = Database()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t0.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t1.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.timestamp: secondary_ds.fields.timestamp}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp, blend_ds.fields.account)
            .widget(ReactTable(blend_ds.fields.metric0, blend_ds.fields.metric1))
        ).sql

        (query,) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq0"."$metric0" "$metric0",'
            '"sq1"."$metric1" "$metric1" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"account" "$account",'
            '"metric" "$metric0" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp","$account"',
            str(query),
        )

    def test_include_mapped_field_in_subqueries_when_the_aliases_are_different(self):
        db = Database()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field("a", label="A", definition=t0.a, data_type=DataType.number,),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field("b", label="B", definition=t1.b, data_type=DataType.number,),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.a: secondary_ds.fields.b}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.a)
            .widget(ReactTable(blend_ds.fields.metric0, blend_ds.fields.metric1))
        ).sql

        (query,) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$a" "$a",'
            '"sq0"."$metric0" "$metric0",'
            '"sq1"."$metric1" "$metric1" '
            "FROM ("
            "SELECT "
            '"a" "$a",'
            '"metric" "$metric0" '
            'FROM "test0" '
            'GROUP BY "$a"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"b" "$b",'
            '"metric" "$metric1" '
            'FROM "test1" '
            'GROUP BY "$b"'
            ') "sq1" ON "sq0"."$a"="sq1"."$b" '
            'ORDER BY "$a"',
            str(query),
        )

    def test_blended_references(self):
        db = TestDatabase()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t0.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t1.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.timestamp: secondary_ds.fields.timestamp}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp, blend_ds.fields.account)
            .widget(ReactTable(blend_ds.fields.metric1))
            .reference(f.DayOverDay(blend_ds.fields.timestamp))
        ).sql

        (query_1, query_2) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq1"."$metric1" "$metric1" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"account" "$account" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp","$account"',
            str(query_1),
        )

        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq1"."$metric1_dod" "$metric1_dod" '
            "FROM ("
            'SELECT TIMESTAMPADD(\'day\',1,"timestamp") "$timestamp",'
            '"account" "$account" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            'SELECT TIMESTAMPADD(\'day\',1,"timestamp") "$timestamp",'
            '"metric" "$metric1_dod" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp","$account"',
            str(query_2),
        )

    def test_blended_references_with_order_by_on_metric(self):
        db = TestDatabase()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t0.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t1.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.timestamp: secondary_ds.fields.timestamp}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp, blend_ds.fields.account)
            .widget(ReactTable(blend_ds.fields.metric1))
            .reference(f.DayOverDay(blend_ds.fields.timestamp))
            .orderby(blend_ds.fields.metric1)
        ).sql

        (query_1, query_2) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq1"."$metric1" "$metric1" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"account" "$account" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$metric1"',
            str(query_1),
        )

        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq1"."$metric1_dod" "$metric1_dod" '
            "FROM ("
            'SELECT TIMESTAMPADD(\'day\',1,"timestamp") "$timestamp",'
            '"account" "$account" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            'SELECT TIMESTAMPADD(\'day\',1,"timestamp") "$timestamp",'
            '"metric" "$metric1_dod" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$metric1_dod"',
            str(query_2),
        )

    def test_blended_references_with_order_by_on_unused_metric(self):
        db = TestDatabase()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t0.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t1.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.timestamp: secondary_ds.fields.timestamp}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp, blend_ds.fields.account)
            .widget(ReactTable(blend_ds.fields.metric1))
            .reference(f.DayOverDay(blend_ds.fields.timestamp))
            .orderby(blend_ds.fields.metric0)
        ).sql

        (query_1, query_2) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq1"."$metric1" "$metric1",'
            '"sq0"."$metric0" "$metric0" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"account" "$account",'
            '"metric" "$metric0" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$metric0"',
            str(query_1),
        )

        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$account" "$account",'
            '"sq1"."$metric1_dod" "$metric1_dod",'
            '"sq0"."$metric0_dod" "$metric0_dod" '
            "FROM ("
            'SELECT TIMESTAMPADD(\'day\',1,"timestamp") "$timestamp",'
            '"account" "$account",'
            '"metric" "$metric0_dod" '
            'FROM "test0" '
            'GROUP BY "$timestamp","$account"'
            ') "sq0" '
            "LEFT JOIN ("
            'SELECT TIMESTAMPADD(\'day\',1,"timestamp") "$timestamp",'
            '"metric" "$metric1_dod" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$metric0_dod"',
            str(query_2),
        )

    def test_blending_with_share_operation_on_primary_metric(self):
        db = TestDatabase()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t0.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t1.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.timestamp: secondary_ds.fields.timestamp}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp)
            .widget(f.Widget(f.Share(blend_ds.fields.metric0, over=blend_ds.fields.timestamp)))
        ).sql

        (query_1, query_2) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$metric0" "$metric0" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric0" '
            'FROM "test0" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(query_1),
        )

        self.assertEqual(
            "SELECT "
            '\'_FIREANT_ROLLUP_VALUE_\' "$timestamp",'
            '"metric" "$metric0" '
            'FROM "test0" '
            'ORDER BY "$timestamp"',
            str(query_2),
        )

    def test_blending_with_share_operation_on_secondary_metric(self):
        db = TestDatabase()
        t0, t1 = Tables("test0", "test1")
        primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t0.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "account",
                    label="Account",
                    definition=t1.account,
                    data_type=DataType.number,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        blend_ds = primary_ds.blend(secondary_ds).on(
            {primary_ds.fields.timestamp: secondary_ds.fields.timestamp}
        )

        sql = (
            blend_ds.query()
            .dimension(blend_ds.fields.timestamp)
            .widget(f.Widget(f.Share(blend_ds.fields.metric1, over=blend_ds.fields.timestamp)))
        ).sql

        (query_1, query_2) = sql
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$metric1" "$metric1" '
            "FROM ("
            "SELECT "
            '"timestamp" "$timestamp" '
            'FROM "test0" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test1" '
            'GROUP BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(query_1),
        )

        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$metric1" "$metric1" '
            "FROM ("
            "SELECT "
            '\'_FIREANT_ROLLUP_VALUE_\' "$timestamp",'
            '"metric" "$metric1" '
            'FROM "test1") "sq0" '
            'ORDER BY "$timestamp"',
            str(query_2),
        )


class MultipleDatasetsBlendedEdgeCaseTests(TestCase):
    @classmethod
    def setUpClass(cls):
        db = Database()
        t0, t1, t2 = Tables("test0", "test1", "test2")
        cls.primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.primary_ds.id = 0
        cls.secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.secondary_ds.id = 1
        cls.tertiary_ds = DataSet(
            table=t2,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t2.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric2",
                    label="Metric2",
                    definition=t2.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.tertiary_ds.id = 2
        cls.blend_ds = (
            cls.primary_ds.blend(cls.secondary_ds).on_dimensions().blend(cls.tertiary_ds).on_dimensions()
        )

    def test_selecting_just_one_metric_in_non_primary_dataset(self):
        blender = self.blend_ds.extra_fields(
            Field(
                "only_metric2",
                label="Metric Two",
                definition=self.tertiary_ds.fields.metric2,
                data_type=DataType.number,
            ),
        )

        (query,) = blender.query().widget(ReactTable(blender.fields.only_metric2)).sql

        self.assertEqual(
            'SELECT "sq0"."$metric2" "$only_metric2" FROM (SELECT "metric" "$metric2" FROM "test2") "sq0"',
            str(query),
        )


class DataSetBlenderMultipleDatasetsTests(TestCase):
    @classmethod
    def setUpClass(cls):
        db = Database()
        t0, t1, t2, t3 = Tables("test0", "test1", "test2", "test3")
        cls.primary_ds = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.primary_ds.id = 0
        cls.secondary_ds = DataSet(
            table=t1,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t1.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric1",
                    label="Metric1",
                    definition=t1.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.secondary_ds.id = 1
        cls.tertiary_ds = DataSet(
            table=t2,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t2.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric2",
                    label="Metric2",
                    definition=t2.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.tertiary_ds.id = 2
        cls.quaternary_ds = DataSet(
            table=t3,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t3.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric3",
                    label="Metric3",
                    definition=t3.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        cls.quaternary_ds.id = 3
        cls.blend_ds = (
            cls.primary_ds.blend(cls.secondary_ds)
            .on_dimensions()
            .blend(cls.tertiary_ds)
            .on_dimensions()
            .blend(cls.quaternary_ds)
            .on_dimensions()
        )

    def _do_test(self, blender):
        self.maxDiff = None

        (query,) = (
            blender.query()
            .dimension(blender.fields.timestamp)
            .widget(ReactTable(blender.fields.metric_share))
        ).sql

        self.assertEqual(
            (
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq0"."$metric0"/"sq1"."$metric1"/"sq2"."$metric2"/"sq3"."$metric3" "$metric_share" '
                "FROM ("
                "SELECT "
                '"timestamp" "$timestamp",'
                '"metric" "$metric0" '
                'FROM "test0" '
                'GROUP BY "$timestamp"'
                ') "sq0",'
                '(SELECT "metric" "$metric2" FROM "test2") "sq2",'
                '(SELECT "metric" "$metric3" FROM "test3") "sq3" '
                "LEFT JOIN ("
                "SELECT "
                '"timestamp" "$timestamp",'
                '"metric" "$metric1" '
                'FROM "test1" '
                'GROUP BY "$timestamp"'
                ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
                'ORDER BY "$timestamp"'
            ),
            str(query),
        )

    def test_dataset_blender_fourway_flattens_on_join_criteria_to_build_on_primary_dataset(
        self,
    ):
        self._do_test(
            self.blend_ds.extra_fields(
                Field(
                    "metric_share",
                    label="Metric Share",
                    definition=self.primary_ds.fields.metric0
                    / self.secondary_ds.fields.metric1
                    / self.tertiary_ds.fields.metric2
                    / self.quaternary_ds.fields.metric3,
                    data_type=DataType.number,
                ),
            )
        )

    def test_dataset_using_fields_refering_top_blender_maps_to_correct_field(self):
        self._do_test(
            self.blend_ds.extra_fields(
                Field(
                    "metric_share",
                    label="Metric Share",
                    definition=self.blend_ds.fields.metric0
                    / self.blend_ds.fields.metric1
                    / self.blend_ds.fields.metric2
                    / self.blend_ds.fields.metric3,
                    data_type=DataType.number,
                ),
            )
        )
