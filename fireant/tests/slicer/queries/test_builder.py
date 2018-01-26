from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from datetime import date

import fireant as f
from fireant.slicer.exceptions import RollupException
from ..matchers import DimensionMatcher
from ..mocks import slicer


class SlicerShortcutTests(TestCase):
    maxDiff = None

    def test_get_attr_from_slicer_dimensions_returns_dimension(self):
        timestamp_dimension = slicer.dimensions.timestamp
        self.assertTrue(hasattr(timestamp_dimension, 'definition'))

    def test_get_attr_from_slicer_metrics_returns_metric(self):
        votes_metric = slicer.metrics.votes
        self.assertTrue(hasattr(votes_metric, 'definition'))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricTests(TestCase):
    maxDiff = None

    def test_build_query_with_single_metric(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician"', query)

    def test_build_query_with_multiple_metrics(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes, slicer.metrics.wins])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician"', query)

    def test_build_query_with_multiple_visualizations(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .widget(f.DataTablesJS([slicer.metrics.wins])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician"', query)

    def test_build_query_for_chart_visualization_with_single_axis(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[
                  f.HighCharts.PieChart(metrics=[slicer.metrics.votes])
              ])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician"', query)

    def test_build_query_for_chart_visualization_with_multiple_axes(self):
        query = slicer.query() \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.PieChart(metrics=[slicer.metrics.votes]))
                    .axis(f.HighCharts.PieChart(metrics=[slicer.metrics.wins]))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician"', query)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTests(TestCase):
    maxDiff = None

    def test_build_query_with_datetime_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_datetime_dimension_hourly(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp(f.hourly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'HH\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_datetime_dimension_daily(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp(f.daily)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_datetime_dimension_weekly(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_datetime_dimension_monthly(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'MM\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_datetime_dimension_quarterly(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Q\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_datetime_dimension_annually(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Y\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_boolean_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.winner) \
            .query

        self.assertEqual('SELECT '
                         '"is_winner" "winner",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "winner" '
                         'ORDER BY "winner"', query)

    def test_build_query_with_categorical_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "political_party" '
                         'ORDER BY "political_party"', query)

    def test_build_query_with_unique_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.election) \
            .query

        self.assertEqual('SELECT '
                         '"election_id" "election",'
                         '"election_year" "election_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "election","election_display" '
                         'ORDER BY "election"', query)

    def test_build_query_with_multiple_dimensions(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display" '
                         'ORDER BY "timestamp","candidate"', query)

    def test_build_query_with_multiple_dimensions_and_visualizations(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes, slicer.metrics.wins])) \
            .widget(f.HighCharts(
              axes=[
                  f.HighCharts.PieChart(metrics=[slicer.metrics.votes]),
                  f.HighCharts.ColumnChart(metrics=[slicer.metrics.wins]),
              ])) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party" '
                         'ORDER BY "timestamp","political_party"', query)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionRollupTests(TestCase):
    maxDiff = None

    def test_build_query_with_rollup_cat_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY ROLLUP("political_party") '
                         'ORDER BY "political_party"', query)

    def test_build_query_with_rollup_uni_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.candidate.rollup()) \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY ROLLUP(("candidate_id","candidate_name")) '
                         'ORDER BY "candidate"', query)

    def test_rollup_following_non_rolled_up_dimensions(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup()) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp",ROLLUP(("candidate_id","candidate_name")) '
                         'ORDER BY "timestamp","candidate"', query)

    def test_force_all_dimensions_following_rollup_to_be_rolled_up(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.political_party.rollup(),
                       slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "political_party",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY ROLLUP("political_party",("candidate_id","candidate_name")) '
                         'ORDER BY "political_party","candidate"', query)

    def test_force_all_dimensions_following_rollup_to_be_rolled_up_with_split_dimension_calls(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .dimension(slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "political_party",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY ROLLUP("political_party",("candidate_id","candidate_name")) '
                         'ORDER BY "political_party","candidate"', query)

    def test_raise_exception_when_trying_to_rollup_continuous_dimension(self):
        with self.assertRaises(RollupException):
            slicer.query() \
                .widget(f.DataTablesJS([slicer.metrics.votes])) \
                .dimension(slicer.dimensions.political_party.rollup(),
                           slicer.dimensions.timestamp) \
                .query

# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_filter_isin_categorical_dim(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.political_party.isin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\')', query)

    def test_build_query_with_filter_notin_categorical_dim(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.political_party.notin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" NOT IN (\'d\')', query)

    def test_build_query_with_filter_isin_unique_dim(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.candidate.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1)', query)

    def test_build_query_with_filter_notin_unique_dim(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.candidate.notin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" NOT IN (1)', query)

    def test_build_query_with_filter_isin_unique_dim_display(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.candidate.isin(['Donald Trump'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" IN (\'Donald Trump\')', query)

    def test_build_query_with_filter_notin_unique_dim_display(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.candidate.notin(['Donald Trump'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT IN (\'Donald Trump\')', query)

    def test_build_query_with_filter_wildcard_unique_dim(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.candidate.wildcard('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\'', query)

    def test_build_query_with_filter_isin_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.query() \
                .widget(f.DataTablesJS([slicer.metrics.votes])) \
                .filter(slicer.dimensions.deepjoin.isin([1], use_display=True))

    def test_build_query_with_filter_notin_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.query() \
                .widget(f.DataTablesJS([slicer.metrics.votes])) \
                .filter(slicer.dimensions.deepjoin.notin([1], use_display=True))

    def test_build_query_with_filter_wildcard_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.query() \
                .widget(f.DataTablesJS([slicer.metrics.votes])) \
                .filter(slicer.dimensions.deepjoin.wildcard('test'))

    def test_build_query_with_filter_range_datetime_dimension(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.timestamp.between(date(2009, 1, 20), date(2017, 1, 20))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2009-01-20\' AND \'2017-01-20\'', query)

    def test_build_query_with_filter_boolean_true(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.winner.is_(True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "is_winner"', query)

    def test_build_query_with_filter_boolean_false(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.winner.is_(False)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT "is_winner"', query)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_metric_filter_eq(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.metrics.votes == 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', query)

    def test_build_query_with_metric_filter_eq_left(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(5 == slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', query)

    def test_build_query_with_metric_filter_ne(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.metrics.votes != 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', query)

    def test_build_query_with_metric_filter_ne_left(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(5 != slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', query)

    def test_build_query_with_metric_filter_gt(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.metrics.votes > 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', query)

    def test_build_query_with_metric_filter_gt_left(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(5 < slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', query)

    def test_build_query_with_metric_filter_gte(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.metrics.votes >= 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', query)

    def test_build_query_with_metric_filter_gte_left(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(5 <= slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', query)

    def test_build_query_with_metric_filter_lt(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.metrics.votes < 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', query)

    def test_build_query_with_metric_filter_lt_left(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(5 > slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', query)

    def test_build_query_with_metric_filter_lte(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.metrics.votes <= 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', query)

    def test_build_query_with_metric_filter_lte_left(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(5 >= slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', query)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderOperationTests(TestCase):
    maxDiff = None

    def test_build_query_with_cumsum_operation(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([f.CumSum(slicer.metrics.votes)])) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)

    def test_build_query_with_cummean_operation(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([f.CumMean(slicer.metrics.votes)])) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', query)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceTests(TestCase):
    maxDiff = None

    def test_dimension_with_single_reference_dod(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.DayOverDay)) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_single_reference_wow(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.WeekOverWeek)) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_wow" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'week\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_single_reference_mom(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.MonthOverMonth)) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_mom" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'month\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_single_reference_qoq(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.QuarterOverQuarter)) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_qoq" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'quarter\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_single_reference_yoy(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.YearOverYear)) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_single_reference_as_a_delta(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.DayOverDay.delta())) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"base"."votes"-"sq1"."votes" "votes_dod_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_single_reference_as_a_delta_percentage(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.DayOverDay.delta(percent=True))) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '("base"."votes"-"sq1"."votes")*100/NULLIF("sq1"."votes",0) "votes_dod_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_dimension_with_multiple_references(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.DayOverDay)
                       .reference(f.YearOverYear.delta(percent=True))) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_dod",'
                         '("base"."votes"-"sq2"."votes")*100/NULLIF("sq2"."votes",0) "votes_yoy_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"sq1"."timestamp") '

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "sq2" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"sq2"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_reference_joins_nested_query_on_dimensions(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.YearOverYear)) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."political_party" "political_party",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"sq1"."timestamp") '
                         'AND "base"."political_party"="sq1"."political_party" '
                         'ORDER BY "timestamp","political_party"', query)

    def test_reference_with_unique_dimension_includes_display_definition(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.YearOverYear)) \
            .dimension(slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."candidate" "candidate",'
                         '"base"."candidate_display" "candidate_display",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"sq1"."timestamp") '
                         'AND "base"."candidate"="sq1"."candidate" '
                         'ORDER BY "timestamp","candidate"', query)

    def test_adjust_reference_dimension_filters_in_reference_query(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.DayOverDay)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_adjust_reference_dimension_filters_in_reference_query_with_multiple_filters(self):
        query = slicer.query() \
            .widget(f.HighCharts(
              axes=[f.HighCharts.LineChart(
                    metrics=[slicer.metrics.votes])])) \
            .dimension(slicer.dimensions.timestamp
                       .reference(f.DayOverDay)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .filter(slicer.dimensions.political_party
                    .isin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         '"base"."timestamp" "timestamp",'
                         '"base"."votes" "votes",'
                         '"sq1"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'AND "political_party" IN (\'d\') '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'LEFT JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'AND "political_party" IN (\'d\') '
                         'GROUP BY "timestamp"'
                         ') "sq1" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"sq1"."timestamp") '
                         'ORDER BY "timestamp"', query)

    def test_references_adapt_for_leap_year(self):
        pass


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderJoinTests(TestCase):
    maxDiff = None

    def test_dimension_with_join_includes_join_in_query(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.district) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "timestamp",'
                         '"politician"."district_id" "district",'
                         '"district"."district_name" "district_display",'
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "timestamp","district","district_display" '
                         'ORDER BY "timestamp","district"', query)

    def test_dimension_with_recursive_join_joins_all_join_tables(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.state) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "timestamp",'
                         '"district"."state_id" "state",'
                         '"state"."state_name" "state_display",'
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'JOIN "locations"."state" '
                         'ON "district"."state_id"="state"."id" '
                         'GROUP BY "timestamp","state","state_display" '
                         'ORDER BY "timestamp","state"', query)

    def test_metric_with_join_includes_join_in_query(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.voters])) \
            .dimension(slicer.dimensions.district) \
            .query

        self.assertEqual('SELECT '
                         '"politician"."district_id" "district",'
                         '"district"."district_name" "district_display",'
                         'COUNT("voter"."id") "voters" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'JOIN "politics"."voter" '
                         'ON "district"."id"="voter"."district_id" '
                         'GROUP BY "district","district_display" '
                         'ORDER BY "district"', query)

    def test_dimension_filter_with_join_on_display_definition_does_not_include_join_in_query(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.district.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "district_id" IN (1)', query)

    def test_dimension_filter_display_field_with_join_includes_join_in_query(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.district.isin(['District 4'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'WHERE "district"."district_name" IN (\'District 4\')', query)

    def test_dimension_filter_with_recursive_join_includes_join_in_query(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.state.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'WHERE "district"."state_id" IN (1)', query)

    def test_dimension_filter_with_deep_recursive_join_includes_joins_in_query(self):
        query = slicer.query() \
            .widget(f.DataTablesJS([slicer.metrics.votes])) \
            .filter(slicer.dimensions.deepjoin.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'JOIN "locations"."state" '
                         'ON "district"."state_id"="state"."id" '
                         'JOIN "test"."deep" '
                         'ON "deep"."id"="state"."ref_id" '
                         'WHERE "deep"."id" IN (1)', query)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderValidationTests(TestCase):
    maxDiff = None

    def test_query_requires_at_least_one_metric(self):
        pass


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuilderRenderTests(TestCase):
    def test_pass_slicer_database_as_arg(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        slicer.query() \
            .widget(mock_widget) \
            .render()

        mock_fetch_data.assert_called_once_with(slicer.database,
                                                ANY,
                                                dimensions=ANY)

    def test_pass_query_from_builder_as_arg(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        slicer.query() \
            .widget(mock_widget) \
            .render()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT SUM("votes") "votes" FROM "politics"."politician"',
                                                dimensions=ANY)

    def test_builder_dimensions_as_arg_with_zero_dimensions(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        slicer.query() \
            .widget(mock_widget) \
            .render()

        mock_fetch_data.assert_called_once_with(ANY, ANY, dimensions=[])

    def test_builder_dimensions_as_arg_with_one_dimension(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        dimensions = [slicer.dimensions.state]

        slicer.query() \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .render()

        mock_fetch_data.assert_called_once_with(ANY, ANY, dimensions=DimensionMatcher(*dimensions))

    def test_builder_dimensions_as_arg_with_multiple_dimensions(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        dimensions = slicer.dimensions.timestamp, slicer.dimensions.state, slicer.dimensions.political_party

        slicer.query() \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .render()

        mock_fetch_data.assert_called_once_with(ANY, ANY, dimensions=DimensionMatcher(*dimensions))

    def test_call_transform_on_widget(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.query() \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .render()

        mock_widget.transform.assert_called_once_with(mock_fetch_data.return_value,
                                                      slicer,
                                                      DimensionMatcher(slicer.dimensions.timestamp))

    def test_returns_results_from_widget_transform(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_widget.metrics = [slicer.metrics.votes]

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        result = slicer.query() \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .render()

        self.assertListEqual(result, [mock_widget.transform.return_value])

    def test_operations_evaluated(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc
        mock_widget.metrics = [mock_operation]
        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.query() \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .render()

        mock_operation.apply.assert_called_once_with(mock_df)

    def test_operations_results_stored_in_data_frame(self, mock_fetch_data: Mock):
        mock_widget = Mock(name='mock_widget')
        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc
        mock_widget.metrics = [mock_operation]
        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.query() \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .render()

        self.assertIn(mock_operation.key, mock_df)
        self.assertEqual(mock_df[mock_operation.key], mock_operation.apply.return_value)
