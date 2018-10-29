from datetime import date
from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTests(TestCase):
    maxDiff = None

    def test_build_query_with_datetime_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_datetime_dimension_hourly(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.hourly)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'HH\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_datetime_dimension_daily(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.daily)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_datetime_dimension_weekly(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_datetime_dimension_monthly(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'MM\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_datetime_dimension_quarterly(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Q\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_datetime_dimension_annually(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Y\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(queries[0]))

    def test_build_query_with_boolean_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.winner) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         '"is_winner" "$d$winner",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$winner" '
                         'ORDER BY "$d$winner"', str(queries[0]))

    def test_build_query_with_categorical_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.political_party) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$political_party" '
                         'ORDER BY "$d$political_party"', str(queries[0]))

    def test_build_query_with_unique_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.election) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         '"election_id" "$d$election",'
                         '"election_year" "$d$election_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$election","$d$election_display" '
                         'ORDER BY "$d$election_display"', str(queries[0]))

    def test_build_query_with_multiple_dimensions(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.candidate) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                         'ORDER BY "$d$timestamp","$d$candidate_display"', str(queries[0]))

    def test_build_query_with_multiple_dimensions_and_visualizations(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes, slicer.metrics.wins)) \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))
                    .axis(f.HighCharts.LineSeries(slicer.metrics.wins))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$political_party" '
                         'ORDER BY "$d$timestamp","$d$political_party"', str(queries[0]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTotalsTests(TestCase):
    maxDiff = None

    def test_build_query_with_totals_cat_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without references or totals'):
            self.assertEqual('SELECT '
                             '"political_party" "$d$political_party",' 
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$political_party" '
                             'ORDER BY "$d$political_party"', str(queries[0]))

        with self.subTest('totals dimension is replaced with NULL'):
            self.assertEqual('SELECT '
                             'NULL "$d$political_party",' 
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'ORDER BY "$d$political_party"', str(queries[1]))

    def test_build_query_with_totals_uni_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.candidate.rollup()) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without references or totals'):
            self.assertEqual('SELECT '
                             '"candidate_id" "$d$candidate",'
                             '"candidate_name" "$d$candidate_display",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$candidate","$d$candidate_display" '
                             'ORDER BY "$d$candidate_display"', str(queries[0]))

        with self.subTest('totals dimension is replaced with NULL'):
            self.assertEqual('SELECT '
                             'NULL "$d$candidate",'
                             'NULL "$d$candidate_display",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'ORDER BY "$d$candidate_display"', str(queries[1]))

    def test_build_query_with_totals_on_dimension_and_subsequent_dimensions(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup(),
                       slicer.dimensions.political_party) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without references or totals'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"candidate_id" "$d$candidate",'
                             '"candidate_name" "$d$candidate_display",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(queries[0]))

        with self.subTest('all dimensions after the rolled up dimension are NULL'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'NULL "$d$candidate",'
                             'NULL "$d$candidate_display",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(queries[1]))

    def test_build_query_with_totals_on_multiple_dimensions_dimension(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup(),
                       slicer.dimensions.political_party.rollup()) \
            .queries

        self.assertEqual(len(queries), 3)

        with self.subTest('base query is same as without references or totals'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"candidate_id" "$d$candidate",'
                             '"candidate_name" "$d$candidate_display",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(queries[0]))

        with self.subTest('all totals dimension are replaced with null'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'NULL "$d$candidate",'
                             'NULL "$d$candidate_display",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(queries[1]))

        with self.subTest('first totals dimension is replaced with null'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"candidate_id" "$d$candidate",'
                             '"candidate_name" "$d$candidate_display",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                             'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(queries[2]))

    def test_build_query_with_totals_cat_dimension_with_references(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.political_party.rollup()) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 4)

        with self.subTest('base query is same as without references or totals'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[0]))

        with self.subTest('reference query is shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[1]))

        with self.subTest('base totals query is same as base query minus the totals dimension'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[2]))

        with self.subTest('reference total query is shifted without the totals dimension'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[3]))

    def test_build_query_with_totals_cat_dimension_with_references_and_date_filters(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2018, 1, 1), date(2019, 1, 1))) \
            .queries

        self.assertEqual(len(queries), 4)

        with self.subTest('base query is same as without references or totals'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                             'GROUP BY "$d$timestamp","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[0]))

        with self.subTest('reference query is shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                             'GROUP BY "$d$timestamp","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[1]))

        with self.subTest('base totals query is same as base query minus the totals dimension'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[2]))

        with self.subTest('reference total query is shifted without the totals dimension'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'NULL "$d$political_party",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[3]))
