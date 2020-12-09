from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import (
    mock_dataset,
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderJoinTests(TestCase):
    maxDiff = None

    def test_dimension_with_join_includes_join_in_query(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.day(mock_dataset.fields.timestamp))
            .dimension(mock_dataset.fields['district-name'])
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
            '"district"."district_name" "$district-name",'
            'SUM("politician"."votes") "$votes" '
            'FROM "politics"."politician" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'GROUP BY "$timestamp","$district-name" '
            'ORDER BY "$timestamp","$district-name" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_with_multiple_joins_includes_joins_ordered__in_query(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes, mock_dataset.fields.voters))
            .dimension(f.day(mock_dataset.fields.timestamp))
            .dimension(mock_dataset.fields['district-name'])
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
            '"district"."district_name" "$district-name",'
            'SUM("politician"."votes") "$votes",'
            'COUNT("voter"."id") "$voters" '
            'FROM "politics"."politician" '
            'JOIN "politics"."voter" '
            'ON "politician"."id"="voter"."politician_id" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'GROUP BY "$timestamp","$district-name" '
            'ORDER BY "$timestamp","$district-name" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_with_recursive_join_joins_all_join_tables(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.day(mock_dataset.fields.timestamp))
            .dimension(mock_dataset.fields.state)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("politician"."timestamp",\'DD\') "$timestamp",'
            '"state"."state_name" "$state",'
            'SUM("politician"."votes") "$votes" '
            'FROM "politics"."politician" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'JOIN "locations"."state" '
            'ON "district"."state_id"="state"."id" '
            'GROUP BY "$timestamp","$state" '
            'ORDER BY "$timestamp","$state" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_metric_with_join_includes_join_in_query(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.voters))
            .dimension(mock_dataset.fields.political_party)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            '"politician"."political_party" "$political_party",'
            'COUNT("voter"."id") "$voters" '
            'FROM "politics"."politician" '
            'JOIN "politics"."voter" '
            'ON "politician"."id"="voter"."politician_id" '
            'GROUP BY "$political_party" '
            'ORDER BY "$political_party" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_filter_with_join_on_display_definition_does_not_include_join_in_query(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields['district-id'].isin([1]))
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'WHERE "district_id" IN (1) '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_filter_with_recursive_join_includes_join_in_query(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields['district-name'].isin(['example']))
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("politician"."votes") "$votes" '
            'FROM "politics"."politician" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'WHERE "district"."district_name" IN (\'example\') '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_dimension_filter_with_deep_recursive_join_includes_joins_in_query(self):
        queries = (
            mock_dataset.query.widget(f.ReactTable(mock_dataset.fields.votes))
            .filter(mock_dataset.fields.deepjoin.isin([1]))
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'SUM("politician"."votes") "$votes" '
            'FROM "politics"."politician" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'JOIN "locations"."state" '
            'ON "district"."state_id"="state"."id" '
            'JOIN "test"."deep" '
            'ON "deep"."id"="state"."ref_id" '
            'WHERE "deep"."id" IN (1) '
            'ORDER BY 1 '
            'LIMIT 200000',
            str(queries[0]),
        )
