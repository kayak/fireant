from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import (
    mock_dataset_blender,
)


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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician"."$votes") "$votes" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'AVG("politician_spend"."$candidate-spend"/"politician"."$wins") "$average-candidate-spend-per-wins" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("is_winner") "$wins" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'AVG("politician_spend"."$candidate-spend"/"politician"."$voters") "$average-candidate-spend-per-voters" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
                         'COUNT("voter"."id") "$voters" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" '
                         'ON "politician"."id"="voter"."politician_id" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_single_field_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['average-candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'AVG("politician_spend"."$candidate-spend") "$average-candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician"."$votes") "$votes" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("votes")>10'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("politician"."$votes")>10 '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("candidate_spend")>500'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("politician_spend"."$candidate-spend")>500 '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician"."$votes") "$votes",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician"."$votes") "$votes",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("votes")>10'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("politician"."$votes")>10 '
                         'ORDER BY "$timestamp"'
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
                         '"politician_spend"."$timestamp" "$timestamp",'
                         'SUM("politician"."$votes") "$votes",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("candidate_spend")>500'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'HAVING SUM("politician_spend"."$candidate-spend")>500 '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

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
                         '"politician"."$candidate-id" "$candidate-id",'
                         '"politician"."$election-year" "$election-year",'
                         'SUM("politician"."$votes") "$votes",'
                         'AVG("politician_spend"."$candidate-spend"/"politician"."$wins") "$average-candidate-spend-per-wins",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         '"candidate_id" "$candidate-id",'
                         '"election_year" "$election-year",'
                         'SUM("votes") "$votes",'
                         'SUM("is_winner") "$wins" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         '"candidate_id" "$candidate-id",'
                         '"election_year" "$election-year",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."candidate_id"="politician_spend"."$candidate-id" AND '
                         '"politician"."election_year"="politician_spend"."$election-year" '
                         'GROUP BY "$candidate-id","$election-year" '
                         'HAVING AVG("politician_spend"."$candidate-spend"/"politician"."$wins")>500 '
                         'ORDER BY "$candidate-id","$election-year"'
        , str(queries[0]))
