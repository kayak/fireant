from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset_blender


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DataSetBlenderQueryBuilderTests(TestCase):
    maxDiff = None

    def test_dimension_with_single_field_metric_from_primary_dataset_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq0"."$votes" "$votes" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_single_field_metric_from_secondary_dataset_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq1"."$candidate-spend" "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_multiple_field_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['average-candidate-spend-per-wins'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq1"."$candidate-spend"/"sq0"."$wins" "$average-candidate-spend-per-wins" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("is_winner") "$wins" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_multiple_field_metric_from_join_in_primary_dataset_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['average-candidate-spend-per-voters'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq1"."$candidate-spend"/"sq0"."$voters" "$average-candidate-spend-per-voters" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
                         'COUNT("voter"."id") "$voters" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" '
                         'ON "politician"."id"="voter"."politician_id" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_a_filter_operating_in_a_primary_dataset_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .filter(mock_dataset_blender.fields['votes'].gt(10)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq0"."$votes" "$votes" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("votes")>10 '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'WHERE "sq0"."$votes">10 '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_a_filter_operating_in_the_secondary_dataset_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .filter(mock_dataset_blender.fields['candidate-spend'].gt(500)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq1"."$candidate-spend" "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("candidate_spend")>500 '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'WHERE "sq1"."$candidate-spend">500 '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_multiple_dimensions_with_a_filter_operating_in_a_dimension_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .filter(mock_dataset_blender.fields['candidate-id'].isin([1])) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq0"."$votes" "$votes",'
                         '"sq1"."$candidate-spend" "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_multiple_dimensions_with_a_filter_operating_in_a_primary_dataset_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .filter(mock_dataset_blender.fields['votes'].gt(10)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq0"."$votes" "$votes",'
                         '"sq1"."$candidate-spend" "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("votes")>10 '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'WHERE "sq0"."$votes">10 '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_multiple_metrics_with_an_order_by_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .orderby(mock_dataset_blender.fields['votes']) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq0"."$votes" "$votes",'
                         '"sq1"."$candidate-spend" "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'ORDER BY "$votes"'
        , str(queries[0]))

    def test_multiple_dimensions_with_a_filter_operating_in_a_secondary_dataset_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .filter(mock_dataset_blender.fields['candidate-spend'].gt(500)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$timestamp" "$timestamp",'
                         '"sq0"."$votes" "$votes",'
                         '"sq1"."$candidate-spend" "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("candidate_spend")>500 '
                         'ORDER BY "$timestamp"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$timestamp"="sq1"."$timestamp" '
                         'WHERE "sq1"."$candidate-spend">500 '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_multiple_dimensions_with_a_reference_in_primary_dataset_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .reference(f.WeekOverWeek(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual(
                             'SELECT '
                             '"sq0"."$timestamp" "$timestamp",'
                             '"sq0"."$votes" "$votes" '
                             'FROM ('
                             'SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq0" '
                             'LEFT JOIN ('
                             'SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp" '
                             'FROM "politics"."politician_spend" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq1" '
                             'ON '
                             '"sq0"."$timestamp"="sq1"."$timestamp" '
                             'ORDER BY "$timestamp"'
            , str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual(
                             'SELECT '
                             '"sq0"."$timestamp" "$timestamp",'
                             '"sq0"."$votes_wow" "$votes_wow" '
                             'FROM ('
                             'SELECT '
                             'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_wow" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq0" '
                             'LEFT JOIN ('
                             'SELECT '
                             'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp" '
                             'FROM "politics"."politician_spend" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq1" '
                             'ON '
                             '"sq0"."$timestamp"="sq1"."$timestamp" '
                             'ORDER BY "$timestamp"'
            , str(queries[1]))

    def test_multiple_dimensions_with_a_reference_in_secondary_dataset_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .reference(f.WeekOverWeek(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual(
                             'SELECT '
                             '"sq0"."$timestamp" "$timestamp",'
                             '"sq1"."$candidate-spend" "$candidate-spend" '
                             'FROM ('
                             'SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq0" '
                             'LEFT JOIN ('
                             'SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("candidate_spend") "$candidate-spend" '
                             'FROM "politics"."politician_spend" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq1" '
                             'ON '
                             '"sq0"."$timestamp"="sq1"."$timestamp" '
                             'ORDER BY "$timestamp"'
            , str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual(
                        'SELECT '
                        '"sq0"."$timestamp" "$timestamp",'
                        '"sq1"."$candidate-spend_wow" "$candidate-spend_wow" '
                        'FROM ('
                        'SELECT '
                         'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp" '
                        'FROM "politics"."politician" '
                        'GROUP BY "$timestamp" '
                        'ORDER BY "$timestamp"'
                        ') "sq0" '
                        'LEFT JOIN ('
                        'SELECT '
                         'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp",'
                        'SUM("candidate_spend") "$candidate-spend_wow" '
                        'FROM "politics"."politician_spend" '
                        'GROUP BY "$timestamp" '
                        'ORDER BY "$timestamp"'
                        ') "sq1" '
                        'ON '
                        '"sq0"."$timestamp"="sq1"."$timestamp" '
                        'ORDER BY "$timestamp"'
            , str(queries[1]))

    def test_multiple_dimensions_with_a_refrence_in_both_datasets_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['votes'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .reference(f.WeekOverWeek(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual(
                             'SELECT '
                             '"sq0"."$timestamp" "$timestamp",'
                             '"sq0"."$votes" "$votes",'
                             '"sq1"."$candidate-spend" "$candidate-spend" '
                             'FROM ('
                             'SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq0" '
                             'LEFT JOIN ('
                             'SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("candidate_spend") "$candidate-spend" '
                             'FROM "politics"."politician_spend" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"'
                             ') "sq1" '
                             'ON '
                             '"sq0"."$timestamp"="sq1"."$timestamp" '
                             'ORDER BY "$timestamp"'
            , str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual(
                        'SELECT '
                        '"sq0"."$timestamp" "$timestamp",'
                        '"sq0"."$votes_wow" "$votes_wow",'
                        '"sq1"."$candidate-spend_wow" "$candidate-spend_wow" '
                        'FROM ('
                        'SELECT '
                         'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp",'
                        'SUM("votes") "$votes_wow" '
                        'FROM "politics"."politician" '
                        'GROUP BY "$timestamp" '
                        'ORDER BY "$timestamp"'
                        ') "sq0" '
                        'LEFT JOIN ('
                        'SELECT '
                         'TRUNC(TIMESTAMPADD(\'week\',1,TRUNC("timestamp",\'DD\')),\'DD\') "$timestamp",'
                        'SUM("candidate_spend") "$candidate-spend_wow" '
                        'FROM "politics"."politician_spend" '
                        'GROUP BY "$timestamp" '
                        'ORDER BY "$timestamp"'
                        ') "sq1" '
                        'ON '
                        '"sq0"."$timestamp"="sq1"."$timestamp" '
                        'ORDER BY "$timestamp"'
            , str(queries[1]))

    def test_multiple_dimensions_with_two_filters_on_different_levels_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields.votes,
                mock_dataset_blender.fields['average-candidate-spend-per-wins'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(mock_dataset_blender.fields['candidate-id']) \
            .dimension(mock_dataset_blender.fields['election-year']) \
            .filter(mock_dataset_blender.fields['candidate-id'].isin([1])) \
            .filter(mock_dataset_blender.fields['average-candidate-spend-per-wins'].gt(500)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"sq0"."$candidate-id" "$candidate-id",'
                         '"sq0"."$election-year" "$election-year",'
                         '"sq0"."$votes" "$votes",'
                         '"sq1"."$candidate-spend" "$candidate-spend",'
                         '"sq1"."$candidate-spend"/"sq0"."$wins" "$average-candidate-spend-per-wins" '
                         'FROM ('
                         'SELECT '
                         '"candidate_id" "$candidate-id",'
                         '"election_year" "$election-year",'
                         'SUM("votes") "$votes",'
                         'SUM("is_winner") "$wins" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year" '
                         'ORDER BY "$candidate-id","$election-year"'
                         ') "sq0" '
                         'LEFT JOIN ('
                         'SELECT '
                         '"candidate_id" "$candidate-id",'
                         '"election_year" "$election-year",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year" '
                         'ORDER BY "$candidate-id","$election-year"'
                         ') "sq1" '
                         'ON '
                         '"sq0"."$candidate-id"="sq1"."$candidate-id" AND '
                         '"sq0"."$election-year"="sq1"."$election-year" '
                         'WHERE "sq1"."$candidate-spend"/"sq0"."$wins">500 '
                         'ORDER BY "$candidate-id","$election-year"'
        , str(queries[0]))
