from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import (
    mock_dataset_blender,
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DataSetBlenderQueryBuilderTests(TestCase):
    maxDiff = None

    def test_dimension_with_single_field_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"politician_spend"."timestamp" "$timestamp",'
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
                         'GROUP BY "$timestamp","$candidate-spend"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_dimension_with_multiple_field_metric_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields['average-candidate-spend-per-candidacy'],
            )) \
            .dimension(f.day(mock_dataset_blender.fields.timestamp)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"politician_spend"."timestamp" "$timestamp",'
                         'AVG("politician_spend"."$candidate-spend"/"politician"."$votes") "$average-candidate-spend-per-candidacy" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$timestamp","$votes"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$timestamp",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'GROUP BY "$timestamp","$candidate-spend"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."timestamp"="politician_spend"."$timestamp" '
                         'GROUP BY "$timestamp" '
                         'ORDER BY "$timestamp"'
        , str(queries[0]))

    def test_multiple_dimensions_with_two_filters_on_different_levels_in_query(self):
        queries = mock_dataset_blender.query \
            .widget(f.ReactTable(
                mock_dataset_blender.fields.votes,
                mock_dataset_blender.fields['average-candidate-spend-per-candidacy'],
                mock_dataset_blender.fields['candidate-spend'],
            )) \
            .dimension(mock_dataset_blender.fields['candidate-id']) \
            .dimension(mock_dataset_blender.fields['election-year']) \
            .filter(mock_dataset_blender.fields['candidate-id'].isin([1])) \
            .filter(mock_dataset_blender.fields['average-candidate-spend-per-candidacy'].gt(500)) \
            .sql

        self.assertEqual(len(queries), 1)

        self.assertEqual(
                         'SELECT '
                         '"politician"."candidate_id" "$candidate-id",'
                         '"politician"."election_year" "$election-year",'
                         'SUM("politician"."votes") "$votes",'
                         'AVG("politician_spend"."$candidate-spend"/"politician"."$votes") "$average-candidate-spend-per-candidacy",'
                         'SUM("politician_spend"."$candidate-spend") "$candidate-spend" '
                         'FROM ('
                         'SELECT '
                         '"candidate_id" "$candidate-id",'
                         '"election_year" "$election-year",'
                         'SUM("votes") "$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year","$votes"'
                         ') "politician" '
                         'LEFT JOIN ('
                         'SELECT '
                         '"candidate_id" "$candidate-id",'
                         '"election_year" "$election-year",'
                         'SUM("candidate_spend") "$candidate-spend" '
                         'FROM "politics"."politician_spend" '
                         'WHERE "candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year","$candidate-spend"'
                         ') "politician_spend" '
                         'ON '
                         '"politician"."candidate_id"="politician_spend"."$candidate-id" AND '
                         '"politician"."election_year"="politician_spend"."$election-year" '
                         'WHERE "politician"."candidate_id" IN (1) '
                         'GROUP BY "$candidate-id","$election-year" '
                         'HAVING AVG("politician_spend"."$candidate-spend"/"politician"."$votes")>500 '
                         'ORDER BY "$candidate-id","$election-year"'
        , str(queries[0]))
