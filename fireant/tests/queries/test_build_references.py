from datetime import date
from unittest import TestCase

import fireant as f
from fireant import Rollup
from fireant.tests.dataset.mocks import mock_dataset

timestamp_daily = f.day(mock_dataset.fields.timestamp)
timestamp_monthly = f.month(mock_dataset.fields.timestamp)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceTests(TestCase):
    maxDiff = None

    def test_reference_with_no_dimensions_or_filters_creates_same_query(self):
        # TODO reduce this to a single query
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                'SELECT SUM("votes") "$votes" FROM "politics"."politician" ORDER BY 1 LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query"):
            self.assertEqual(
                'SELECT SUM("votes") "$votes_dod" FROM "politics"."politician" ORDER BY 1 LIMIT 200000',
                str(queries[1]),
            )

    def test_reference_without_selecting_ref_dimension_using_date_range_filter(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(mock_dataset.fields.political_party)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .filter(mock_dataset.fields.timestamp.between(date(2000, 1, 1), date(2000, 3, 1)))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2000-01-01' AND '2000-03-01' "
                'GROUP BY "$political_party" '
                'ORDER BY "$political_party" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('day',-1,'2000-01-01') "
                "AND TIMESTAMPADD('day',-1,'2000-03-01') "
                'GROUP BY "$political_party" '
                'ORDER BY "$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_dod(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_wow(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.WeekOverWeek(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_wow" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_mom(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.MonthOverMonth(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',4,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_mom" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_qoq(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.QuarterOverQuarter(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',12,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_qoq" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_yoy(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.YearOverYear(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',52,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_yoy" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_metric_filters_get_filtered_out(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .filter(mock_dataset.fields.votes > 1)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query has the metric filter"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'HAVING SUM("votes")>1 '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query does not include the metric filter"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_mom_with_monthly_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_monthly)
            .reference(f.MonthOverMonth(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'MM\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('month',1,TRUNC(\"timestamp\",'MM')),'MM') \"$timestamp\","
                'SUM("votes") "$votes_mom" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_qoq_with_monthly_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_monthly)
            .reference(f.QuarterOverQuarter(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'MM\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('quarter',1,TRUNC(\"timestamp\",'MM')),'MM') \"$timestamp\","
                'SUM("votes") "$votes_qoq" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_yoy_with_monthly_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_monthly)
            .reference(f.YearOverYear(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'MM\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('year',1,TRUNC(\"timestamp\",'MM')),'MM') \"$timestamp\","
                'SUM("votes") "$votes_yoy" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceWithDeltaTests(TestCase):
    maxDiff = None

    def test_delta(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp, delta=True))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_delta_percentage(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp, delta_percent=True))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceIntervalTests(TestCase):
    maxDiff = None

    def test_date_dim_with_weekly_interval(self):
        weekly_timestamp = f.week(mock_dataset.fields.timestamp)
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(weekly_timestamp)
            .reference(f.DayOverDay(weekly_timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'IW\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'IW')),'IW') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_date_dim_with_weekly_interval_no_interval_on_reference(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(f.week(mock_dataset.fields.timestamp))
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'IW\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'IW')),'IW') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_date_dim_with_monthly_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(f.month(mock_dataset.fields.timestamp))
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'MM\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'MM')),'MM') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_date_dim_with_quarterly_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(f.quarter(mock_dataset.fields.timestamp))
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'Q\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'Q')),'Q') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_date_dim_with_annual_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(f.year(mock_dataset.fields.timestamp))
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'Y\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'Y')),'Y') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeMultipleReferencesTests(TestCase):
    maxDiff = None

    def test_dimension_with_multiple_references(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .reference(f.YearOverYear(mock_dataset.fields.timestamp, delta_percent=True))
            .sql
        )

        self.assertEqual(3, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',52,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_yoy" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[2]),
            )

    def test_adding_duplicate_reference_does_not_join_more_queries(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(
                f.DayOverDay(mock_dataset.fields.timestamp),
                f.DayOverDay(mock_dataset.fields.timestamp),
            )
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension(
        self,
    ):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(
                f.DayOverDay(mock_dataset.fields.timestamp),
                f.DayOverDay(mock_dataset.fields.timestamp, delta=True),
                f.DayOverDay(mock_dataset.fields.timestamp, delta_percent=True),
            )
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension_with_different_periods(
        self,
    ):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(
                f.DayOverDay(mock_dataset.fields.timestamp),
                f.DayOverDay(mock_dataset.fields.timestamp, delta=True),
                # also work with modified dimensions
                f.YearOverYear(timestamp_daily),
                f.YearOverYear(timestamp_daily, delta=True),
            )
            .sql
        )

        self.assertEqual(3, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("second query for all DoD references"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

        with self.subTest("third query for all YoY references"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',52,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_yoy" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[2]),
            )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceMiscellaneousTests(TestCase):
    maxDiff = None

    def test_reference_queries_with_multiple_dimensions_includes_all_dimensions(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .dimension(mock_dataset.fields.political_party)
            .reference(f.YearOverYear(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"political_party" "$political_party",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$political_party" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',52,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                '"political_party" "$political_party",'
                'SUM("votes") "$votes_yoy" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$political_party" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_reference_with_dimension_using_display_definition_includes_it_in_all_queries(
        self,
    ):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .dimension(mock_dataset.fields["candidate-name"])
            .reference(f.YearOverYear(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"candidate_name" "$candidate-name",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-name" '
                'ORDER BY "$timestamp","$candidate-name" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',52,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                '"candidate_name" "$candidate-name",'
                'SUM("votes") "$votes_yoy" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-name" '
                'ORDER BY "$timestamp","$candidate-name" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_filters_on_reference_dimension_are_adapted_to_reference_interval(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .filter(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2018, 1, 31)))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2018-01-31' "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('day',-1,'2018-01-01') "
                "AND TIMESTAMPADD('day',-1,'2018-01-31') "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_filters_on_other_dimensions_are_not_adapted(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(timestamp_daily)
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .filter(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2018, 1, 31)))
            .filter(mock_dataset.fields.political_party.isin(["d"]))
            .sql
        )

        self.assertEqual(2, len(queries))

        with self.subTest("base query is same as without reference"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2018-01-31' "
                "AND \"political_party\" IN ('d') "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is same as base query with filter on reference dimension shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('day',-1,'2018-01-01') AND TIMESTAMPADD('day',-1,'2018-01-31') "
                "AND \"political_party\" IN ('d') "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderReferencesWithRollupTests(TestCase):
    maxDiff = None

    def test_reference_with_rollup_dimension_and_date_range_filter(self):
        queries = (
            mock_dataset.query.widget(f.HighCharts().axis(f.HighCharts.LineSeries(mock_dataset.fields.votes)))
            .dimension(Rollup(timestamp_daily))
            .reference(f.WeekOverWeek(mock_dataset.fields.timestamp))
            .filter(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2018, 1, 31)))
            .sql
        )

        self.assertEqual(4, len(queries))

        base, reference, base_rollup, reference_rollup = queries

        with self.subTest("base query applies dimensions and date range filter"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2018-01-31' "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(base),
            )

        with self.subTest("reference query shifts timestamp dimension and date range filter by a week"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('week',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                'SUM("votes") "$votes_wow" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('week',-1,'2018-01-01') "
                "AND TIMESTAMPADD('week',-1,'2018-01-31') "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(reference),
            )

        with self.subTest("totals query selects _FIREANT_ROLLUP_VALUE_ for timestamp dimension"):
            self.assertEqual(
                "SELECT "
                "'_FIREANT_ROLLUP_VALUE_' \"$timestamp\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2018-01-31' "
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(base_rollup),
            )

        with self.subTest(
            "reference totals query selects _FIREANT_ROLLUP_VALUE_ for timestamp dimension and shifts date range filter"
        ):
            self.assertEqual(
                "SELECT "
                "'_FIREANT_ROLLUP_VALUE_' \"$timestamp\","
                'SUM("votes") "$votes_wow" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('week',-1,'2018-01-01') "
                "AND TIMESTAMPADD('week',-1,'2018-01-31') "
                'ORDER BY "$timestamp" '
                'LIMIT 200000',
                str(reference_rollup),
            )
