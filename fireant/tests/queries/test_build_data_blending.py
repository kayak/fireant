import copy
from unittest import TestCase

from pypika import Order

import fireant as f
from fireant.tests.dataset.mocks import (
    Rollup,
    TestMySQLDatabase,
    mock_dataset_blender,
    mock_staff_dataset,
)


class DataSetBlenderQueryBuilderTests(TestCase):
    maxDiff = None

    def test_using_fields_from_single_dataset_reduced_to_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["votes"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            'SELECT "sq0"."$timestamp" "$timestamp","sq0"."$votes" "$votes" '
            'FROM ('
            'SELECT TRUNC("timestamp",\'DD\') "$timestamp",SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp"'
            ') "sq0" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_db_specific_querybuilder_class_used_when_needed(self):
        dataset_blender = copy.deepcopy(mock_dataset_blender)
        blender = (
            dataset_blender.query()
            .widget(
                f.ReactTable(
                    dataset_blender.fields["candidate-spend"],
                    dataset_blender.fields["voters"],
                )
            )
            .dimension(f.day(dataset_blender.fields.timestamp))
        )

        # Given all mocks are based on the Vertica database, this is a quick override to avoid a lot of duplicate mocks!
        blender.dataset.primary_dataset.database = TestMySQLDatabase()

        queries = blender.sql
        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            "`sq0`.`$timestamp` `$timestamp`,"
            "`sq1`.`$candidate-spend` `$candidate-spend`,"
            "`sq0`.`$voters` `$voters` "
            "FROM ("
            "SELECT DATE_FORMAT(`politician`.`timestamp`,'%Y-%m-%d 00:00:00') `$timestamp`,"
            "COUNT(`voter`.`id`) `$voters` "
            "FROM `politics`.`politician` "
            "JOIN `politics`.`voter` "
            "ON `politician`.`id`=`voter`.`politician_id` "
            "GROUP BY `$timestamp`) `sq0` "
            "LEFT JOIN ("
            "SELECT TRUNC(`timestamp`,'DD') `$timestamp`,"
            "SUM(`candidate_spend`) `$candidate-spend` "
            "FROM `politics`.`politician_spend` "
            "GROUP BY `$timestamp`"
            ") `sq1` "
            "ON `sq0`.`$timestamp`=`sq1`.`$timestamp` "
            "ORDER BY `$timestamp` "
            "LIMIT 200000",
            str(queries[0]),
        )

    def test_using_datablender_metric_builds_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_using_datablender_builds_query_with_mapped_and_unmapped_dimensions(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            'ORDER BY "$timestamp","$political_party" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_metric_filter_to_dataset_field_filters_in_nested_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_metric_in_primary_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(f.ResultSet(mock_dataset_blender.fields["votes"].gt(10)))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$set(SUM(votes)>10)" "$set(SUM(votes)>10)",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'CASE WHEN SUM("votes")>10 THEN \'set(SUM(votes)>10)\' ELSE \'complement(SUM(votes)>10)\' END "$set(SUM(votes)>10)",'
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_metric_in_secondary_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(f.ResultSet(mock_dataset_blender.fields["candidate-spend"].gt(500)))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq1"."$set(SUM(candidate_spend)>500)" "$set(SUM(candidate_spend)>500)",'
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
            'CASE WHEN SUM("candidate_spend")>500 THEN \'set(SUM(candidate_spend)>500)\' '
            'ELSE \'complement(SUM(candidate_spend)>500)\' '
            'END "$set(SUM(candidate_spend)>500)",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_dimension_in_tertiary_dataset_query(self):
        blender_dataset_with_staff = mock_dataset_blender.blend(mock_staff_dataset).on_dimensions()

        queries = (
            blender_dataset_with_staff.query()
            .widget(f.ReactTable(blender_dataset_with_staff.fields.num_staff))
            .dimension(blender_dataset_with_staff.fields['political_party'])
            .filter(f.ResultSet(blender_dataset_with_staff.fields["candidate-id"] == 12))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$political_party" "$political_party",'
            '"sq0"."$candidate-id" "$candidate-id",'
            '"sq1"."$num_staff" "$num_staff" '
            "FROM ("
            "SELECT "
            '"political_party" "$political_party",'
            'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id" '
            'FROM "politics"."politician" '
            'GROUP BY "$political_party","$candidate-id"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
            'COUNT("staff_id") "$num_staff" '
            'FROM "politics"."politician_staff" '
            'GROUP BY "$candidate-id"'
            ') "sq1" '
            "ON "
            '"sq0"."$candidate-id"="sq1"."$candidate-id" '
            'ORDER BY "$political_party","$candidate-id" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_dimension_that_is_also_being_fetched_in_tertiary_dataset_query(self):
        blender_dataset_with_staff = mock_dataset_blender.blend(mock_staff_dataset).on_dimensions()

        queries = (
            blender_dataset_with_staff.query()
            .widget(f.ReactTable(blender_dataset_with_staff.fields.num_staff))
            .dimension(blender_dataset_with_staff.fields['candidate-id'])
            .filter(f.ResultSet(blender_dataset_with_staff.fields["candidate-id"] == 12))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            'SELECT "sq0"."$candidate-id" "$candidate-id","sq0"."$num_staff" "$num_staff" '
            'FROM ('
            'SELECT CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' '
            'ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
            'COUNT("staff_id") "$num_staff" '
            'FROM "politics"."politician_staff" '
            'GROUP BY "$candidate-id"'
            ') "sq0" '
            'ORDER BY "$candidate-id" LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_metric_in_blender_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(f.ResultSet(mock_dataset_blender.fields["candidate-spend-per-wins"].gt(1000)))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            'CASE WHEN "sq1"."$candidate-spend"/"sq0"."$wins">1000 THEN \'set(sq1.$candidate-spend/sq0.$wins>1000)\' '
            'ELSE \'complement(sq1.$candidate-spend/sq0.$wins>1000)\' '
            'END \"$set(sq1.$candidate-spend/sq0.$wins>1000)\",'
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_metric_that_is_also_being_fetched_in_blender_dataset_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(
                f.ReactTable(mock_dataset_blender.fields["votes"], mock_dataset_blender.fields["candidate-spend"]),
            )
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(f.ResultSet(mock_dataset_blender.fields['votes'] > 10000))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$set(SUM(votes)>10000)" "$set(SUM(votes)>10000)",'
            '"sq0"."$votes" "$votes",'
            '"sq1"."$candidate-spend" "$candidate-spend" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'CASE WHEN SUM("votes")>10000 THEN \'set(SUM(votes)>10000)\' '
            'ELSE \'complement(SUM(votes)>10000)\' '
            'END "$set(SUM(votes)>10000)",'
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_dimension_in_both_dataset_queries(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .filter(f.ResultSet(mock_dataset_blender.fields['candidate-id'] == 12))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$timestamp" "$timestamp",'
            '"sq0"."$candidate-id" "$candidate-id",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$candidate-id"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp","$candidate-id"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
            'ORDER BY "$timestamp","$candidate-id" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_set_filter_for_dimension_with_reference_in_both_dataset_queries(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
            .dimension(f.day(mock_dataset_blender.fields.timestamp))
            .reference(f.WeekOverWeek(mock_dataset_blender.fields.timestamp))
            .filter(f.ResultSet(mock_dataset_blender.fields['candidate-id'] == 12))
        ).sql

        self.assertEqual(len(queries), 2)

        with self.subTest("base query"):
            self.assertEqual(
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq0"."$candidate-id" "$candidate-id",'
                '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
                "FROM ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
                'SUM("is_winner") "$wins" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
                'SUM("candidate_spend") "$candidate-spend" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
                'ORDER BY "$timestamp","$candidate-id" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("ref query"):
            self.assertEqual(
                "SELECT "
                '"sq0"."$timestamp" "$timestamp",'
                '"sq0"."$candidate-id" "$candidate-id",'
                '"sq1"."$candidate-spend_wow"/"sq0"."$wins_wow" "$candidate-spend-per-wins_wow" '
                "FROM ("
                "SELECT "
                'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp",'
                'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
                'SUM("is_winner") "$wins_wow" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp",'
                'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
                'SUM("candidate_spend") "$candidate-spend_wow" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp","$candidate-id"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
                'ORDER BY "$timestamp","$candidate-id" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_apply_set_filter_for_dimension_that_is_also_being_fetched_in_both_dataset_queries(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
            .dimension(mock_dataset_blender.fields['candidate-id'])
            .filter(f.ResultSet(mock_dataset_blender.fields['candidate-id'] == 12))
        ).sql

        self.assertEqual(len(queries), 1)
        self.assertEqual(
            "SELECT "
            '"sq0"."$candidate-id" "$candidate-id",'
            '"sq1"."$candidate-spend"/"sq0"."$wins" "$candidate-spend-per-wins" '
            "FROM ("
            "SELECT "
            'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$candidate-id"'
            ') "sq0" '
            "LEFT JOIN ("
            "SELECT "
            'CASE WHEN "candidate_id"=12 THEN \'set(candidate_id=12)\' ELSE \'complement(candidate_id=12)\' END "$candidate-id",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$candidate-id"'
            ') "sq1" '
            "ON "
            '"sq0"."$candidate-id"="sq1"."$candidate-id" '
            'ORDER BY "$candidate-id" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_filter_variations_with_sets_for_data_blending(self):
        for field_alias, fltr in [
            ('state', mock_dataset_blender.fields.state.like("%abc%")),
            ('state', mock_dataset_blender.fields.state.not_like("%abc%")),
            ('state', mock_dataset_blender.fields.state.isin(["abc"])),
            ('state', mock_dataset_blender.fields.state.notin(["abc"])),
            ('timestamp', mock_dataset_blender.fields.timestamp.between('date1', 'date2')),
            ('candidate-id', mock_dataset_blender.fields['candidate-id'].between(5, 15)),
            ('candidate-id', mock_dataset_blender.fields['candidate-id'].isin([1, 2, 3])),
            ('candidate-id', mock_dataset_blender.fields['candidate-id'].notin([1, 2, 3])),
        ]:
            fltr_definition = fltr.definition
            while hasattr(fltr_definition, 'definition'):
                fltr_definition = fltr_definition.definition

            fltr_sql = fltr_definition.get_sql(quote_char="")

            with self.subTest(fltr_sql):
                queries = (
                    mock_dataset_blender.query.widget(f.Pandas(mock_dataset_blender.fields['candidate-spend']))
                    .dimension(mock_dataset_blender.fields[field_alias])
                    .filter(f.ResultSet(fltr, set_label='set_A', complement_label='set_B'))
                    .sql
                )

                self.assertEqual(len(queries), 1)
                self.assertEqual(
                    "SELECT "
                    f'"sq0"."${field_alias}" "${field_alias}",'
                    '"sq0"."$candidate-spend" "$candidate-spend" '
                    'FROM ('
                    'SELECT '
                    f'CASE WHEN {fltr} THEN \'set_A\' ELSE \'set_B\' END "${field_alias}",'
                    'SUM("candidate_spend") "$candidate-spend" '
                    'FROM "politics"."politician_spend" '
                    f'GROUP BY "${field_alias}"'
                    f') "sq0" '
                    f"ORDER BY \"${field_alias}\" "
                    "LIMIT 200000",
                    str(queries[0]),
                )

    def test_apply_dimension_filter_on_mapped_dimension_field_filters_in_both_nested_query(
        self,
    ):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_apply_dimension_filter_on_unmapped_dimension_field_filters_in_dataset_nested_query(
        self,
    ):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
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
            'ORDER BY "$votes" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_blend_data_set_on_query_using_joins(self):
        query = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            '"state_name" "$state",'
            'SUM("candidate_spend") "$candidate-spend" '
            'FROM "politics"."politician_spend" '
            'GROUP BY "$timestamp","$state"'
            ') "sq1" '
            "ON "
            '"sq0"."$timestamp"="sq1"."$timestamp" '
            'AND "sq0"."$state"="sq1"."$state" '
            'ORDER BY "$timestamp","$state" '
            'LIMIT 200000',
            str(query.sql[0]),
        )

    def test_apply_reference_to_blended_query(self):
        query = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
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
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(ref_query),
            )

    def test_apply_totals_to_blended_query(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
                'ORDER BY "$timestamp","$candidate-id" '
                'LIMIT 200000',
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
                'GROUP BY "$timestamp"'
                ') "sq0" '
                "LEFT JOIN ("
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$candidate-id\","
                'SUM("candidate_spend") "$candidate-spend" '
                'FROM "politics"."politician_spend" '
                'GROUP BY "$timestamp"'
                ') "sq1" '
                "ON "
                '"sq0"."$timestamp"="sq1"."$timestamp" '
                'AND "sq0"."$candidate-id"="sq1"."$candidate-id" '
                'ORDER BY "$timestamp","$candidate-id" '
                'LIMIT 200000',
                str(totals_query),
            )

    def test_blended_query_with_orderby_mapped_dimension(self):
        queries = (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["candidate-spend-per-wins"]))
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
            'ORDER BY "$candidate-id" DESC '
            'LIMIT 200000',
            str(query),
        )

    def test_does_not_raise_SlicerException_when_a_dimension_is_not_mapped_for_unnecessary_secondary_datasets(
        self,
    ):
        # noinspection PyStatementEffect
        (
            mock_dataset_blender.query()
            .widget(f.ReactTable(mock_dataset_blender.fields["votes"]))
            .dimension(mock_dataset_blender.fields["district-id"])
        ).sql
