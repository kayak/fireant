from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import Rollup, mock_dataset_blender
from pypika import Order


class DataSetBlenderQueryBuilderTests(TestCase):
    maxDiff = None

    def test_using_fields_from_single_dataset_reduced_to_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["votes"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
        ).sql

        self.assertEqual(len(queries), 1)
        # TODO Optimisation opportunity with desired result:
        # self.assertEqual(
        #     "SELECT "
        #     'TRUNC("timestamp",\'DD\') "$timestamp",'
        #     'SUM("votes") "$votes" '
        #     'FROM "politics"."politician" '
        #     'GROUP BY "$timestamp" '
        #     'ORDER BY "$timestamp"',
        #     str(queries[0]),
        # )
        self.assertEqual(
            'SELECT "sq0"."$timestamp" "$timestamp","sq0"."$votes" "$votes" '
            "FROM ("
            'SELECT TRUNC("timestamp",\'DD\') "$timestamp",SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp") "sq0" '
            "LEFT JOIN ("
            'SELECT TRUNC("timestamp",\'DD\') "$timestamp" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp") "sq1" '
            'ON "sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(queries[0]),
        )

    def test_fields_from_multiple_datasets_results_in_blender_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(
                    mock_dataset_blender.fields["candidate-spend"],
                    mock_dataset_blender.fields["voters"],
                )
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$candidate-spend" "$candidate-spend",'
            '"sq0"."$voters" "$voters" '
            "FROM ("
            "SELECT "
            'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
            'COUNT("voter"."id") "$voters" '
            'FROM "politics"."politician" '
            'JOIN "politics"."voter" ON "politician"."id"="voter"."politician_id" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(queries[0]),
        )

    def test_using_datablender_metric_builds_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(queries[0]),
        )

    def test_using_datablender_builds_query_with_mapped_and_unmapped_dimensions(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(
                f.day(mock_dataset_blender.fields.timestamp),
                mock_dataset_blender.fields.political_party,
            )
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$political_party" "$political_party",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"political_party" "$political_party",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$political_party"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp","$political_party"',
            str(queries[0]),
        )

    def test_apply_metric_filter_to_dataset_field_filters_in_nested_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(mock_dataset_blender.fields["votes"].gt(10))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'HAVING SUM("votes")>10'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(queries[0]),
        )

    def test_apply_dimension_filter_on_mapped_dimension_field_filters_in_both_nested_query(
        self,
    ):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(mock_dataset_blender.fields["candidate-id"] == 1)
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'WHERE "candidate_id"=1 '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'WHERE "candidate_id"=1 '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(queries[0]),
        )

    def test_apply_dimension_filter_on_UNmapped_dimension_field_filters_in_dataset_nested_query(
        self,
    ):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(mock_dataset_blender.fields["political_party"] == "d")
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            "WHERE \"political_party\"='d' "
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp"',
            str(queries[0]),
        )

    def test_multiple_metrics_with_an_order_by_in_query_applies_order_to_wrapping_query(
        self,
    ):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(
                    mock_dataset_blender.fields["votes"],
                    mock_dataset_blender.fields["candidate-spend"],
                )
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .orderby(mock_dataset_blender.fields["votes"])
        ).sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$votes" "$votes",'
            '"sq1"."$candidate-spend" "$candidate-spend" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$votes"',
            str(queries[0]),
        )

    def test_blend_data_set_on_query_using_joins(self):
        query = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(
                f.day(mock_dataset_blender.fields.timestamp),
                mock_dataset_blender.fields.state,
            )
        )

        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$state" "$state",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
            '"state"."state_name" "$state",'
            'SUM("politician"."is_winner") "$wins" '
            'FROM "politics"."politician" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'JOIN "locations"."state" '
            'ON "district"."state_id"="state"."id" '
            'GROUP BY "$timestamp","$state"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"state" "$state",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp","$state"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'AND "sq0"."$state"="sq1"."$state" '
            'ORDER BY "$timestamp","$state"',
            str(query.sql[0]),
        )

    def test_apply_reference_to_blended_query(self):
        query = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .reference(f.WeekOverWeek(mock_dataset_blender.fields.timestamp))
        )

        sql = query.sql

        self.assertEqual(len(sql), 2)
        (base_query, ref_query) = sql
        with self.subTest("base query"):
            self.assertEqual(
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
                "FROM ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("is_winner") "$wins" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("candidate_spend") "$candidate-spend" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'ORDER BY "$timestamp"',
                str(base_query),
            )
        with self.subTest("ref query"):
            self.assertEqual(
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq1"."$candidate-spend_wow"/"sq0"."$wins_wow" "$candidate-spend-per-wins_wow" '
                "FROM ("
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("is_winner") "$wins_wow" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("candidate_spend") "$candidate-spend_wow" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'ORDER BY "$timestamp"',
                str(ref_query),
            )

    def test_apply_totals_to_blended_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(
                f.day(mock_dataset_blender.fields.timestamp),
                Rollup(mock_dataset_blender.fields["candidate-id"]),
            )
        ).sql

        self.assertEqual(len(queries), 2)
        (base_query, totals_query) = queries
        with self.subTest("base query"):
            self.assertEqual(
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq0"."$candidate-id" "$candidate-id",'
                '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
                "FROM ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"candidate_id" "$candidate-id",'
                'SUM("is_winner") "$wins" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"candidate_id" "$candidate-id",'
                'SUM("candidate_spend") "$candidate-spend" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
                'ORDER BY "$timestamp","$candidate-id"',
                str(base_query),
            )
        with self.subTest("totals query"):
            self.assertEqual(
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq0"."$candidate-id" "$candidate-id",'
                '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
                "FROM ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$candidate-id\","
                'SUM("is_winner") "$wins" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$candidate-id\","
                'SUM("candidate_spend") "$candidate-spend" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
                'ORDER BY "$timestamp","$candidate-id"',
                str(totals_query),
            )

    def test_blended_query_with_orderby_mapped_dimension(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"])
            )
            .dimension(
                f.day(mock_dataset_blender.fields.timestamp),
                mock_dataset_blender.fields["candidate-id"],
            )
            .orderby(mock_dataset_blender.fields["candidate-id"], Order.desc)
        ).sql

        self.assertEqual(len(queries), 1)
        (query,) = queries
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$candidate-id" "$candidate-id",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"candidate_id" "$candidate-id",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$candidate-id"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"candidate_id" "$candidate-id",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp","$candidate-id"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
            'ORDER BY "$candidate-id" DESC',
            str(query),
        )

    def test_does_not_raise_SlicerException_when_a_dimension_is_not_mapped_for_unnecessary_secondary_datasets(
        self,
    ):
        (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["votes"]))
            .dimension(mock_dataset_blender.fields["district-id"])
        ).sql
