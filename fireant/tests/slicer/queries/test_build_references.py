from datetime import date
from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceTests(TestCase):
    maxDiff = None

    def test_single_reference_dod_with_no_dimension_uses_multiple_from_clauses_instead_of_joins(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician"', str(queries[0]))

        with self.subTest('reference query is same as base query'):
            self.assertEqual('SELECT '
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician"', str(queries[1]))

    def test_single_reference_dod_with_dimension_but_not_reference_dimension_in_query_using_filter(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.political_party) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2000, 1, 1), date(2000, 3, 1))) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                             'GROUP BY "$d$political_party" '
                             'ORDER BY "$d$political_party"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                             'GROUP BY "$d$political_party" '
                             'ORDER BY "$d$political_party"', str(queries[1]))

    def test_dimension_with_single_reference_dod(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_wow(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.WeekOverWeek(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'week\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_wow" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_mom(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.MonthOverMonth(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'month\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_mom" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_qoq(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.QuarterOverQuarter(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'quarter\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_qoq" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_yoy(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceWithDeltaTests(TestCase):
    maxDiff = None

    def test_dimension_with_single_reference_as_a_delta(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp, delta=True)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_as_a_delta_percentage(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp, delta_percent=True)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceIntervalTests(TestCase):
    maxDiff = None

    def test_reference_on_dimension_with_weekly_interval(self):
        weekly_timestamp = slicer.dimensions.timestamp(f.weekly)
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(weekly_timestamp) \
            .reference(f.DayOverDay(weekly_timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'IW\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_weekly_interval_no_interval_on_reference(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'IW\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_monthly_interval(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'MM\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'MM\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_quarterly_interval(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'Q\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'Q\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_annual_interval(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'Y\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'Y\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeMultipleReferencesTests(TestCase):
    maxDiff = None

    def test_dimension_with_multiple_references(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta_percent=True)) \
            .queries

        self.assertEqual(len(queries), 3)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[2]))

    def test_adding_duplicate_reference_does_not_join_more_queries(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp, delta=True),
                       f.DayOverDay(slicer.dimensions.timestamp, delta_percent=True)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension_with_different_periods(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp, delta=True),
                       f.YearOverYear(slicer.dimensions.timestamp),
                       f.YearOverYear(slicer.dimensions.timestamp, delta=True)) \
            .queries

        self.assertEqual(len(queries), 3)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[2]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceMiscellaneousTests(TestCase):
    maxDiff = None

    def test_reference_queries_with_multiple_dimensions_includes_all_dimensions(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             '"political_party" "$d$political_party",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$political_party" '
                             'ORDER BY "$d$timestamp","$d$political_party"', str(queries[1]))

    def test_reference_with_dimension_using_display_definition_includes_it_in_all_queries(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.candidate) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             '"candidate_id" "$d$candidate",'
                             '"candidate_name" "$d$candidate_display",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                             'ORDER BY "$d$timestamp","$d$candidate_display"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             '"candidate_id" "$d$candidate",'
                             '"candidate_name" "$d$candidate_display",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                             'ORDER BY "$d$timestamp","$d$candidate_display"', str(queries[1]))

    def test_filters_on_reference_dimension_are_adapted_to_reference_interval(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_filters_on_other_dimensions_are_not_adapted(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .filter(slicer.dimensions.political_party
                    .isin(['d'])) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'AND "political_party" IN (\'d\') '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$d$timestamp",'
                             'SUM("votes") "$m$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'AND "political_party" IN (\'d\') '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceWithLeapYearTests(TestCase):
    maxDiff = None

    def test_adapt_dow_for_leap_year_for_yoy_reference(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) '
                             '"$d$timestamp",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))

    def test_adapt_dow_for_leap_year_for_yoy_reference_with_date_filter(self):
        queries = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2018, 1, 1), date(2018, 1, 31))) \
            .queries

        self.assertEqual(len(queries), 2)

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                             'SUM("votes") "$m$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) '
                             '"$d$timestamp",'
                             'SUM("votes") "$m$votes_yoy" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'year\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$d$timestamp" '
                             'ORDER BY "$d$timestamp"', str(queries[1]))
