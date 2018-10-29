from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderJoinTests(TestCase):
    maxDiff = None

    def test_dimension_with_join_includes_join_in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.district) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "$d$timestamp",'
                         '"politician"."district_id" "$d$district",'
                         '"district"."district_name" "$d$district_display",'
                         'SUM("politician"."votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "$d$timestamp","$d$district","$d$district_display" '
                         'ORDER BY "$d$timestamp","$d$district_display"', str(queries[0]))

    def test_dimension_with_multiple_joins_includes_joins_ordered__in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes,
                                   slicer.metrics.voters)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.district) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "$d$timestamp",'
                         '"politician"."district_id" "$d$district",'
                         '"district"."district_name" "$d$district_display",'
                         'SUM("politician"."votes") "$m$votes",'
                         'COUNT("voter"."id") "$m$voters" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" '
                         'ON "politician"."id"="voter"."politician_id" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "$d$timestamp","$d$district","$d$district_display" '
                         'ORDER BY "$d$timestamp","$d$district_display"', str(queries[0]))

    def test_dimension_with_recursive_join_joins_all_join_tables(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.state) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "$d$timestamp",'
                         '"district"."state_id" "$d$state",'
                         '"state"."state_name" "$d$state_display",'
                         'SUM("politician"."votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'JOIN "locations"."state" '
                         'ON "district"."state_id"="state"."id" '
                         'GROUP BY "$d$timestamp","$d$state","$d$state_display" '
                         'ORDER BY "$d$timestamp","$d$state_display"', str(queries[0]))

    def test_metric_with_join_includes_join_in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.voters)) \
            .dimension(slicer.dimensions.political_party) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         '"politician"."political_party" "$d$political_party",'
                         'COUNT("voter"."id") "$m$voters" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" '
                         'ON "politician"."id"="voter"."politician_id" '
                         'GROUP BY "$d$political_party" '
                         'ORDER BY "$d$political_party"', str(queries[0]))

    def test_dimension_filter_with_join_on_display_definition_does_not_include_join_in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.district.isin([1])) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "district_id" IN (1)', str(queries[0]))

    def test_dimension_filter_display_field_with_join_includes_join_in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.district.display.isin(['District 4'])) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'WHERE "district"."district_name" IN (\'District 4\')', str(queries[0]))

    def test_dimension_filter_with_recursive_join_includes_join_in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.state.isin([1])) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'WHERE "district"."state_id" IN (1)', str(queries[0]))

    def test_dimension_filter_with_deep_recursive_join_includes_joins_in_query(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.deepjoin.isin([1])) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'JOIN "locations"."state" '
                         'ON "district"."state_id"="state"."id" '
                         'JOIN "test"."deep" '
                         'ON "deep"."id"="state"."ref_id" '
                         'WHERE "deep"."id" IN (1)', str(queries[0]))
