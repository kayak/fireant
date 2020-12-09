from datetime import date
from unittest import TestCase

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTests(TestCase):
    maxDiff = None

    def test_build_query_with_datetime_no_interval(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(mock_dataset.fields.timestamp)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            '"timestamp" "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_datetime_interval_hourly(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.hour(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            'TRUNC("timestamp",\'HH\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_datetime_interval_daily(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.day(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 1)

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

    def test_build_query_with_datetime_interval_weekly(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.week(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 1)

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

    def test_build_query_with_datetime_interval_monthly(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.month(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 1)

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

    def test_build_query_with_datetime_interval_quarterly(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.quarter(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 1)

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

    def test_build_query_with_datetime_interval_annually(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.year(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 1)

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

    def test_build_query_with_boolean_type_dimension(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(mock_dataset.fields.winner)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            '"is_winner" "$winner",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$winner" '
            'ORDER BY "$winner" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_dimension(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(mock_dataset.fields.political_party)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            '"political_party" "$political_party",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$political_party" '
            'ORDER BY "$political_party" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_multiple_dimensions(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.day(mock_dataset.fields.timestamp))
            .dimension(mock_dataset.fields["candidate-id"])
            .dimension(mock_dataset.fields["candidate-name"])
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"candidate_id" "$candidate-id",'
            '"candidate_name" "$candidate-name",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$candidate-id","$candidate-name" '
            'ORDER BY "$timestamp","$candidate-id","$candidate-name" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_with_multiple_dimensions_and_visualizations(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes, mock_dataset.fields.wins))
            .widget(
                f.HighCharts()
                .axis(f.HighCharts.LineSeries(mock_dataset.fields.votes))
                .axis(f.HighCharts.LineSeries(mock_dataset.fields.wins))
            )
            .dimension(f.day(mock_dataset.fields.timestamp))
            .dimension(mock_dataset.fields.political_party)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            "SELECT "
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"political_party" "$political_party",'
            'SUM("votes") "$votes",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$political_party" '
            'ORDER BY "$timestamp","$political_party" '
            'LIMIT 200000',
            str(queries[0]),
        )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTotalsTests(TestCase):
    maxDiff = None

    def test_build_query_with_single_rollup_dimension(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.Rollup(mock_dataset.fields.political_party))
            .sql
        )

        self.assertEqual(len(queries), 2)

        with self.subTest("base query is same as without references or totals"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$political_party" '
                'ORDER BY "$political_party" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("totals dimension is replaced with _FIREANT_ROLLUP_VALUE_"):
            self.assertEqual(
                "SELECT "
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'ORDER BY "$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_build_query_with_totals_on_dimension_and_subsequent_dimensions(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(
                f.day(mock_dataset.fields.timestamp),
                f.Rollup(mock_dataset.fields["candidate-id"]),
                mock_dataset.fields.political_party,
            )
            .sql
        )

        self.assertEqual(len(queries), 2)

        with self.subTest("base query is same as without references or totals"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"candidate_id" "$candidate-id",'
                '"political_party" "$political_party",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id","$political_party" '
                'ORDER BY "$timestamp","$candidate-id","$political_party" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("all dimensions after the rolled up dimension are _FIREANT_ROLLUP_VALUE_"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$candidate-id\","
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp","$candidate-id","$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_build_query_with_rollup_multiple_dimensions(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(
                f.day(mock_dataset.fields.timestamp),
                f.Rollup(mock_dataset.fields["candidate-id"]),
                f.Rollup(mock_dataset.fields.political_party),
            )
            .sql
        )

        self.assertEqual(len(queries), 3)

        with self.subTest("base query is same as without references or rollup"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"candidate_id" "$candidate-id",'
                '"political_party" "$political_party",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id","$political_party" '
                'ORDER BY "$timestamp","$candidate-id","$political_party" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("in first rollup dimension's query, the dimension is replaced with _FIREANT_ROLLUP_VALUE_"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"candidate_id" "$candidate-id",'
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$candidate-id" '
                'ORDER BY "$timestamp","$candidate-id","$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

        with self.subTest(
            "in the second rollup dimension's query, rollup dimension and all following dimensions "
            "are replaced with _FIREANT_ROLLUP_VALUE_"
        ):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$candidate-id\","
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp","$candidate-id","$political_party" '
                'LIMIT 200000',
                str(queries[2]),
            )

    def test_build_query_with_rollup_dimension_and_a_reference(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(
                f.day(mock_dataset.fields.timestamp),
                f.Rollup(mock_dataset.fields.political_party),
            )
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .sql
        )

        self.assertEqual(len(queries), 4)

        with self.subTest("base query is same as without references or rollup"):
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

        with self.subTest("reference query is shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                '"political_party" "$political_party",'
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp","$political_party" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

        with self.subTest("base rollup query is same as base query minus the rollup dimension"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[2]),
            )

        with self.subTest("reference total query is shifted without the rollup dimension"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[3]),
            )

    def test_build_query_with_rollup_cat_dimension_with_references_and_date_filters(
        self,
    ):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(f.day(mock_dataset.fields.timestamp))
            .dimension(f.Rollup(mock_dataset.fields.political_party))
            .reference(f.DayOverDay(mock_dataset.fields.timestamp))
            .filter(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2019, 1, 1)))
            .sql
        )

        self.assertEqual(len(queries), 4)

        with self.subTest("base query is same as without references or rollup"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                '"political_party" "$political_party",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2019-01-01' "
                'GROUP BY "$timestamp","$political_party" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("reference query is shifted"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                '"political_party" "$political_party",'
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('day',-1,'2018-01-01') "
                "AND TIMESTAMPADD('day',-1,'2019-01-01') "
                'GROUP BY "$timestamp","$political_party" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[1]),
            )

        with self.subTest("base rollup query is same as base query minus the rollup dimension"):
            self.assertEqual(
                "SELECT "
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2019-01-01' "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[2]),
            )

        with self.subTest("reference total query is shifted without the rollup dimension"):
            self.assertEqual(
                "SELECT "
                "TRUNC(TIMESTAMPADD('day',1,TRUNC(\"timestamp\",'DD')),'DD') \"$timestamp\","
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                'SUM("votes") "$votes_dod" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN TIMESTAMPADD('day',-1,'2018-01-01') "
                "AND TIMESTAMPADD('day',-1,'2019-01-01') "
                'GROUP BY "$timestamp" '
                'ORDER BY "$timestamp","$political_party" '
                'LIMIT 200000',
                str(queries[3]),
            )

    def test_build_query_with_rollup_dimension_and_total_filter_not_applied(self):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(
                mock_dataset.fields.political_party,
                f.Rollup(f.day(mock_dataset.fields.timestamp)),
            )
            .filter(f.OmitFromRollup(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2019, 1, 1))))
            .sql
        )

        self.assertEqual(len(queries), 2)

        with self.subTest("base query is same as without rollup"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2019-01-01' "
                'GROUP BY "$political_party","$timestamp" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("base rollup query is same as base query minus the rollup dimension without filter"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                "'_FIREANT_ROLLUP_VALUE_' \"$timestamp\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$political_party" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_build_query_with_rollup_dimension_and_two_filters_only_one_applied_to_rollup(
        self,
    ):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(
                mock_dataset.fields.political_party,
                f.Rollup(f.day(mock_dataset.fields.timestamp)),
            )
            .filter(
                f.OmitFromRollup(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2019, 1, 1))),
                mock_dataset.fields.timestamp.between(date(2018, 3, 1), date(2019, 9, 1)),
            )
            .sql
        )

        self.assertEqual(len(queries), 2)

        with self.subTest("base query is same as without rollup with both filters"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2019-01-01' "
                "AND \"timestamp\" BETWEEN '2018-03-01' AND '2019-09-01' "
                'GROUP BY "$political_party","$timestamp" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("base rollup query is same as base query minus the rollup dimension with one filter"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                "'_FIREANT_ROLLUP_VALUE_' \"$timestamp\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-03-01' AND '2019-09-01' "
                'GROUP BY "$political_party" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

    def test_build_query_with_rollup_dimensions_and_filter_applied_on_correct_rollup_dimension(
        self,
    ):
        queries = (
            mock_dataset.query()
            .widget(f.ReactTable(mock_dataset.fields.votes))
            .dimension(
                f.Rollup(mock_dataset.fields.political_party),
                f.Rollup(f.day(mock_dataset.fields.timestamp)),
            )
            .filter(f.OmitFromRollup(mock_dataset.fields.timestamp.between(date(2018, 1, 1), date(2019, 1, 1))))
            .sql
        )

        self.assertEqual(len(queries), 3)

        with self.subTest("base query is same as without rollup"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                'TRUNC("timestamp",\'DD\') "$timestamp",'
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                "WHERE \"timestamp\" BETWEEN '2018-01-01' AND '2019-01-01' "
                'GROUP BY "$political_party","$timestamp" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[0]),
            )

        with self.subTest("base rollup query is same as base query minus the timestamp rollup dimension"):
            self.assertEqual(
                "SELECT "
                '"political_party" "$political_party",'
                "'_FIREANT_ROLLUP_VALUE_' \"$timestamp\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'GROUP BY "$political_party" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[1]),
            )

        with self.subTest("base rollup query is same as base query minus the political party rollup dimension"):
            self.assertEqual(
                "SELECT "
                "'_FIREANT_ROLLUP_VALUE_' \"$political_party\","
                "'_FIREANT_ROLLUP_VALUE_' \"$timestamp\","
                'SUM("votes") "$votes" '
                'FROM "politics"."politician" '
                'ORDER BY "$political_party","$timestamp" '
                'LIMIT 200000',
                str(queries[2]),
            )
