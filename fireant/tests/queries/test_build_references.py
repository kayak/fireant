from unittest import TestCase

from datetime import date

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset

timestamp_daily = f.day(mock_dataset.fields.timestamp)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceTests(TestCase):
    maxDiff = None

    def test_single_reference_dod_with_no_dimension_uses_multiple_from_clauses_instead_of_joins(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician"', str(queries[0]))

        with self.subTest('reference query is same as base query'):
            self.assertEqual('SELECT '
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician"', str(queries[1]))

    def test_single_reference_dod_with_dimension_but_not_reference_dimension_in_query_using_filter(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(mock_dataset.fields.political_party) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .filter(mock_dataset.fields.timestamp.between(date(2000, 1, 1), date(2000, 3, 1))) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             '"political_party" "$political_party",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                             'GROUP BY "$political_party" '
                             'ORDER BY "$political_party"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             '"political_party" "$political_party",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                             'GROUP BY "$political_party" '
                             'ORDER BY "$political_party"', str(queries[1]))

    def test_dimension_with_single_reference_dod(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_wow(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.WeekOverWeek(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'week\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_wow" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_mom(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.MonthOverMonth(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'month\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_mom" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_qoq(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.QuarterOverQuarter(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'quarter\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_qoq" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_yoy(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.YearOverYear(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceWithDeltaTests(TestCase):
    maxDiff = None

    def test_dimension_with_single_reference_as_a_delta(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp, delta=True)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_dimension_with_single_reference_as_a_delta_percentage(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp, delta_percent=True)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceIntervalTests(TestCase):
    maxDiff = None

    def test_reference_on_dimension_with_weekly_interval(self):
        weekly_timestamp = f.week(mock_dataset.fields.timestamp)
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(weekly_timestamp) \
            .reference(f.DayOverDay(weekly_timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'IW\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_weekly_interval_no_interval_on_reference(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(f.week(mock_dataset.fields.timestamp)) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'IW\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_monthly_interval(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(f.month(mock_dataset.fields.timestamp)) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'MM\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'MM\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_quarterly_interval(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(f.quarter(mock_dataset.fields.timestamp)) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'Q\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'Q\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_reference_on_dimension_with_annual_interval(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(f.year(mock_dataset.fields.timestamp)) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'Y\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'Y\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeMultipleReferencesTests(TestCase):
    maxDiff = None

    def test_dimension_with_multiple_references(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .reference(f.YearOverYear(mock_dataset.fields.timestamp, delta_percent=True)) \
            .sql

        self.assertEqual(3, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[2]))

    def test_adding_duplicate_reference_does_not_join_more_queries(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp),
                       f.DayOverDay(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp),
                       f.DayOverDay(mock_dataset.fields.timestamp, delta=True),
                       f.DayOverDay(mock_dataset.fields.timestamp, delta_percent=True)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension_with_different_periods(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp),
                       f.DayOverDay(mock_dataset.fields.timestamp, delta=True),
                       f.YearOverYear(mock_dataset.fields.timestamp),
                       f.YearOverYear(mock_dataset.fields.timestamp, delta=True)) \
            .sql

        self.assertEqual(3, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[2]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceMiscellaneousTests(TestCase):
    maxDiff = None

    def test_reference_queries_with_multiple_dimensions_includes_all_dimensions(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .dimension(mock_dataset.fields.political_party) \
            .reference(f.YearOverYear(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             '"political_party" "$political_party",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp","$political_party" '
                             'ORDER BY "$timestamp","$political_party"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$timestamp",'
                             '"political_party" "$political_party",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp","$political_party" '
                             'ORDER BY "$timestamp","$political_party"', str(queries[1]))

    def test_reference_with_dimension_using_display_definition_includes_it_in_all_queries(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .dimension(mock_dataset.fields['candidate-name']) \
            .reference(f.YearOverYear(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             '"candidate_name" "$candidate-name",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp","$candidate-name" '
                             'ORDER BY "$timestamp","$candidate-name"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'DD\') "$timestamp",'
                             '"candidate_name" "$candidate-name",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp","$candidate-name" '
                             'ORDER BY "$timestamp","$candidate-name"', str(queries[1]))

    def test_filters_on_reference_dimension_are_adapted_to_reference_interval(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .filter(mock_dataset.fields.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_filters_on_other_dimensions_are_not_adapted(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(timestamp_daily) \
            .reference(f.DayOverDay(mock_dataset.fields.timestamp)) \
            .filter(mock_dataset.fields.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .filter(mock_dataset.fields.political_party
                    .isin(['d'])) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'DD\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'AND "political_party" IN (\'d\') '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TRUNC(TIMESTAMPADD(\'day\',1,"timestamp"),\'DD\') "$timestamp",'
                             'SUM("votes") "$votes_dod" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'AND "political_party" IN (\'d\') '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceWithLeapYearTests(TestCase):
    maxDiff = None

    def test_adapt_dow_for_leap_year_for_yoy_reference(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(f.week(mock_dataset.fields.timestamp)) \
            .reference(f.YearOverYear(mock_dataset.fields.timestamp)) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) '
                             '"$timestamp",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))

    def test_adapt_dow_for_leap_year_for_yoy_reference_with_date_filter(self):
        queries = mock_dataset.query \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))) \
            .dimension(f.week(mock_dataset.fields.timestamp)) \
            .reference(f.YearOverYear(mock_dataset.fields.timestamp)) \
            .filter(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2018, 1, 31))) \
            .sql

        self.assertEqual(2, len(queries))

        with self.subTest('base query is same as without reference'):
            self.assertEqual('SELECT '
                             'TRUNC("timestamp",\'IW\') "$timestamp",'
                             'SUM("votes") "$votes" '
                             'FROM "politics"."politician" '
                             'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[0]))

        with self.subTest('reference query is same as base query with filter on reference dimension shifted'):
            self.assertEqual('SELECT '
                             'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) '
                             '"$timestamp",'
                             'SUM("votes") "$votes_yoy" '
                             'FROM "politics"."politician" '
                             'WHERE TIMESTAMPADD(\'year\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                             'GROUP BY "$timestamp" '
                             'ORDER BY "$timestamp"', str(queries[1]))
