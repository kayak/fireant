from unittest import TestCase

from fireant import (
    DataSet,
    DataType,
    Database,
    Field,
    ReactTable,
)
from pypika import Tables


class DataSetBlenderCornerCasesTests(TestCase):
    maxDiff = None

    def test_conflicting_metric_aliases_in_blended_datasets(self):
        db = Database()
        t0, t1 = Tables("test0", "test1")
        base_ds = DataSet(
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
        blend_ds = (
            base_ds.blend(secondary_ds)
            .on_dimensions()
            .extra_fields(
                Field(
                    "metric_share",
                    label="Metric Share",
                    definition=base_ds.fields.metric / secondary_ds.fields.metric,
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
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            '"timestamp" "$timestamp",'
            '"metric" "$metric" '
            'FROM "test0" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp"'
            ') "sq1" ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(query),
        )
