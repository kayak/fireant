from datetime import date
from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from pypika import Order

import fireant as f
from fireant.slicer.exceptions import (
    MetricRequiredException,
)
from ..matchers import (
    DimensionMatcher,
)
from ..mocks import slicer


class QueryBuilderTests(TestCase):
    def test_widget_is_immutable(self):
        query1 = slicer.data
        query2 = query1.widget(f.DataTablesJS(slicer.metrics.votes))

        self.assertIsNot(query1, query2)

    def test_dimension_is_immutable(self):
        query1 = slicer.data
        query2 = query1.dimension(slicer.dimensions.timestamp)

        self.assertIsNot(query1, query2)

    def test_filter_is_immutable(self):
        query1 = slicer.data
        query2 = query1.filter(slicer.dimensions.timestamp == 'ok')

        self.assertIsNot(query1, query2)

    def test_orderby_is_immutable(self):
        query1 = slicer.data
        query2 = query1.orderby(slicer.dimensions.timestamp)

        self.assertIsNot(query1, query2)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricTests(TestCase):
    maxDiff = None

    def test_build_query_with_single_metric(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_with_multiple_metrics(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes, slicer.metrics.wins)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_with_multiple_visualizations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .widget(f.DataTablesJS(slicer.metrics.wins)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_for_chart_visualization_with_single_axis(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician"', str(query))

    def test_build_query_for_chart_visualization_with_multiple_axes(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))
                    .axis(f.HighCharts.LineChart(slicer.metrics.wins))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes",'
                         'SUM("is_winner") "wins" '
                         'FROM "politics"."politician"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTests(TestCase):
    maxDiff = None

    def test_build_query_with_datetime_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_datetime_dimension_hourly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.hourly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'HH\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_datetime_dimension_daily(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.daily)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_datetime_dimension_weekly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_datetime_dimension_monthly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'MM\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_datetime_dimension_quarterly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Q\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_datetime_dimension_annually(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Y\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_boolean_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.winner) \
            .query

        self.assertEqual('SELECT '
                         '"is_winner" "winner",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "winner" '
                         'ORDER BY "winner"', str(query))

    def test_build_query_with_categorical_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "political_party" '
                         'ORDER BY "political_party"', str(query))

    def test_build_query_with_unique_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.election) \
            .query

        self.assertEqual('SELECT '
                         '"election_id" "election",'
                         '"election_year" "election_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "election","election_display" '
                         'ORDER BY "election_display"', str(query))

    def test_build_query_with_pattern_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.pattern(['groupA%', 'groupB%'])) \
            .query

        self.assertEqual('SELECT '
                         'CASE '
                         'WHEN "pattern" LIKE \'groupA%\' THEN \'groupA%\' '
                         'WHEN "pattern" LIKE \'groupB%\' THEN \'groupB%\' '
                         'END "pattern",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "pattern" '
                         'ORDER BY "pattern"', str(query))

    def test_build_query_with_pattern_no_values(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.pattern) \
            .query

        self.assertEqual('SELECT '
                         '\'No Group\' "pattern",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "pattern" '
                         'ORDER BY "pattern"', str(query))

    def test_build_query_with_multiple_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
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
                         'ORDER BY "timestamp","candidate_display"', str(query))

    def test_build_query_with_multiple_dimensions_and_visualizations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes, slicer.metrics.wins)) \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))
                    .axis(f.HighCharts.LineChart(slicer.metrics.wins))) \
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
                         'ORDER BY "timestamp","political_party"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTotalsTests(TestCase):
    maxDiff = None

    def test_build_query_with_totals_cat_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .query

        self.assertEqual('(SELECT '
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'NULL "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician") '

                         'ORDER BY "political_party"', str(query))

    def test_build_query_with_totals_uni_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.candidate.rollup()) \
            .query

        self.assertEqual('(SELECT '
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "candidate","candidate_display") '

                         'UNION ALL '

                         '(SELECT '
                         'NULL "candidate",'
                         'NULL "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician") '
        
                         'ORDER BY "candidate_display"', str(query))

    def test_build_query_with_totals_on_dimension_and_subsequent_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup(),
                       slicer.dimensions.political_party) \
            .query

        self.assertEqual('(SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display","political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'NULL "candidate",'
                         'NULL "candidate_display",'
                         'NULL "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp") '
                         'ORDER BY "timestamp","candidate_display","political_party"', str(query))

    def test_build_query_with_totals_on_multiple_dimensions_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup(),
                       slicer.dimensions.political_party.rollup()) \
            .query

        self.assertEqual('(SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display","political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'NULL "candidate",'
                         'NULL "candidate_display",'
                         'NULL "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp") '

                         'UNION ALL '

                         '(SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'NULL "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display") '

                         'ORDER BY "timestamp","candidate_display","political_party"', str(query))

    def test_build_query_with_totals_cat_dimension_with_references(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.political_party.rollup()) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        # Important that in reference queries when using totals that the null dimensions are omitted from the nested
        # queries and selected in the container query
        self.assertEqual('(SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         'COALESCE("base"."political_party","dod"."political_party") "political_party",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM ('

                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party"'
                         ') "base" '

                         'FULL OUTER JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party"'
                         ') "dod" '
                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'AND "base"."political_party"="dod"."political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         'NULL "political_party",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '

                         'FULL OUTER JOIN ('
                         'SELECT TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '
                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp")) '
                         'ORDER BY "timestamp","political_party"', str(query))

    def test_build_query_with_totals_cat_dimension_with_references_and_date_filters(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2018, 1, 1), date(2019, 1, 1))) \
            .query

        self.assertEqual('(SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         'COALESCE("base"."political_party","dod"."political_party") "political_party",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM ('

                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "timestamp","political_party"'
                         ') "base" '

                         'FULL OUTER JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "timestamp","political_party"'
                         ') "dod" '
                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'AND "base"."political_party"="dod"."political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         'NULL "political_party",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "timestamp"'
                         ') "base" '

                         'FULL OUTER JOIN ('
                         'SELECT TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "timestamp"'
                         ') "dod" '
                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp")) '
                         'ORDER BY "timestamp","political_party"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_filter_isin_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.isin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\')', str(query))

    def test_build_query_with_filter_notin_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.notin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" NOT IN (\'d\')', str(query))

    def test_build_query_with_filter_isin_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1)', str(query))

    def test_build_query_with_filter_notin_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.notin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" NOT IN (1)', str(query))

    def test_build_query_with_filter_isin_unique_dim_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.isin(['Donald Trump'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" IN (\'Donald Trump\')', str(query))

    def test_build_query_with_filter_notin_unique_dim_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.notin(['Donald Trump'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT IN (\'Donald Trump\')', str(query))

    def test_build_query_with_filter_like_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_like_display_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_not_like_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.not_like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_isin_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.data \
                .widget(f.DataTablesJS(slicer.metrics.votes)) \
                .filter(slicer.dimensions.deepjoin.isin([1], use_display=True))

    def test_build_query_with_filter_notin_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.data \
                .widget(f.DataTablesJS(slicer.metrics.votes)) \
                .filter(slicer.dimensions.deepjoin.notin([1], use_display=True))

    def test_build_query_with_filter_like_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.data \
                .widget(f.DataTablesJS(slicer.metrics.votes)) \
                .filter(slicer.dimensions.deepjoin.like('test'))

    def test_build_query_with_filter_not_like_raise_exception_when_display_definition_undefined(self):
        with self.assertRaises(f.QueryException):
            slicer.data \
                .widget(f.DataTablesJS(slicer.metrics.votes)) \
                .filter(slicer.dimensions.deepjoin.not_like('test'))

    def test_build_query_with_filter_range_datetime_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.timestamp.between(date(2009, 1, 20), date(2017, 1, 20))) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2009-01-20\' AND \'2017-01-20\'', str(query))

    def test_build_query_with_filter_boolean_true(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.winner.is_(True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "is_winner"', str(query))

    def test_build_query_with_filter_boolean_false(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.winner.is_(False)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT "is_winner"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_metric_filter_eq(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes == 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', str(query))

    def test_build_query_with_metric_filter_eq_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 == slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', str(query))

    def test_build_query_with_metric_filter_ne(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes != 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', str(query))

    def test_build_query_with_metric_filter_ne_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 != slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', str(query))

    def test_build_query_with_metric_filter_gt(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes > 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', str(query))

    def test_build_query_with_metric_filter_gt_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 < slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', str(query))

    def test_build_query_with_metric_filter_gte(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes >= 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', str(query))

    def test_build_query_with_metric_filter_gte_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 <= slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', str(query))

    def test_build_query_with_metric_filter_lt(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes < 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', str(query))

    def test_build_query_with_metric_filter_lt_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 > slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', str(query))

    def test_build_query_with_metric_filter_lte(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes <= 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', str(query))

    def test_build_query_with_metric_filter_lte_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 >= slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderOperationTests(TestCase):
    maxDiff = None

    def test_build_query_with_cumsum_operation(self):
        query = slicer.data \
            .widget(f.DataTablesJS(f.CumSum(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_with_cummean_operation(self):
        query = slicer.data \
            .widget(f.DataTablesJS(f.CumMean(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceTests(TestCase):
    maxDiff = None

    def test_single_reference_dod_with_no_dimension_uses_multiple_from_clauses_instead_of_joins(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '

                         'FROM ('
                         'SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician"'
                         ') "base",('
                         'SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician"'
                         ') "dod"', str(query))

    def test_single_reference_dod_with_dimension_but_not_reference_dimension_in_query_using_filter(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.political_party) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2000, 1, 1), date(2000, 3, 1))) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."political_party","dod"."political_party") "political_party",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                         'GROUP BY "political_party"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                         'GROUP BY "political_party"'
                         ') "dod" '  # end-nested

                         'ON "base"."political_party"="dod"."political_party" '
                         'ORDER BY "political_party"', str(query))

    def test_dimension_with_single_reference_dod(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_single_reference_wow(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.WeekOverWeek(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'week\',1,"wow"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"wow"."votes" "votes_wow" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "wow" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'week\',1,"wow"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_single_reference_mom(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.MonthOverMonth(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'month\',1,"mom"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"mom"."votes" "votes_mom" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "mom" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'month\',1,"mom"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_single_reference_qoq(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.QuarterOverQuarter(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'quarter\',1,"qoq"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"qoq"."votes" "votes_qoq" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "qoq" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'quarter\',1,"qoq"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_single_reference_yoy(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"yoy"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_single_reference_as_a_delta(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp, delta=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"base"."votes"-"dod"."votes" "votes_dod_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_single_reference_as_a_delta_percentage(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '("base"."votes"-"dod"."votes")*100/NULLIF("dod"."votes",0) "votes_dod_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_reference_on_dimension_with_weekly_interval(self):
        weekly_timestamp = slicer.dimensions.timestamp(f.weekly)
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(weekly_timestamp) \
            .reference(f.DayOverDay(weekly_timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_reference_on_dimension_with_weekly_interval_no_interval_on_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_reference_on_dimension_with_monthly_interval(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'MM\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'MM\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_reference_on_dimension_with_quarterly_interval(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Q\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Q\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_reference_on_dimension_with_annual_interval(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Y\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Y\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_dimension_with_multiple_references(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
        
                         'COALESCE('
                         '"base"."timestamp",'
                         'TIMESTAMPADD(\'day\',1,"dod"."timestamp"),'
                         'TIMESTAMPADD(\'year\',1,"yoy"."timestamp")'
                         ') "timestamp",'
        
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod",'
                         '("base"."votes"-"yoy"."votes")*100/NULLIF("yoy"."votes",0) "votes_yoy_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_reference_joins_nested_query_on_dimensions(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         'COALESCE("base"."political_party","yoy"."political_party") "political_party",'
                         '"base"."votes" "votes",'
                         '"yoy"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"political_party" "political_party",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","political_party"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'AND "base"."political_party"="yoy"."political_party" '
                         'ORDER BY "timestamp","political_party"', str(query))

    def test_reference_with_unique_dimension_includes_display_definition(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.candidate) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         'COALESCE("base"."candidate","yoy"."candidate") "candidate",'
                         'COALESCE("base"."candidate_display","yoy"."candidate_display") "candidate_display",'
                         '"base"."votes" "votes",'
                         '"yoy"."votes" "votes_yoy" '
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

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'AND "base"."candidate"="yoy"."candidate" '
                         'ORDER BY "timestamp","candidate_display"', str(query))

    def test_adjust_reference_dimension_filters_in_reference_query(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_adjust_reference_dimension_filters_in_reference_query_with_multiple_filters(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .filter(slicer.dimensions.political_party
                    .isin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
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

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'AND "political_party" IN (\'d\') '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"yoy"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_delta_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"base"."votes"-"yoy"."votes" "votes_yoy_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_delta_percent_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '("base"."votes"-"yoy"."votes")*100/NULLIF("yoy"."votes",0) "votes_yoy_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_reference_with_date_filter(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2018, 1, 1), date(2018, 1, 31))) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'year\',1,"yoy"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"yoy"."votes" "votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'year\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_adding_duplicate_reference_does_not_join_more_queries(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp, delta=True),
                       f.DayOverDay(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("base"."timestamp",TIMESTAMPADD(\'day\',1,"dod"."timestamp")) "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod",'
                         '"base"."votes"-"dod"."votes" "votes_dod_delta",'
                         '("base"."votes"-"dod"."votes")*100/NULLIF("dod"."votes",0) "votes_dod_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '
                         'ORDER BY "timestamp"', str(query))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension_with_different_periods(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineChart(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp, delta=True),
                       f.YearOverYear(slicer.dimensions.timestamp),
                       f.YearOverYear(slicer.dimensions.timestamp, delta=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE('
                         '"base"."timestamp",'
                         'TIMESTAMPADD(\'day\',1,"dod"."timestamp"),'
                         'TIMESTAMPADD(\'year\',1,"yoy"."timestamp")'
                         ') "timestamp",'
                         '"base"."votes" "votes",'
                         '"dod"."votes" "votes_dod",'
                         '"base"."votes"-"dod"."votes" "votes_dod_delta",'
                         '"yoy"."votes" "votes_yoy",'
                         '"base"."votes"-"yoy"."votes" "votes_yoy_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "dod" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'day\',1,"dod"."timestamp") '

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp"'
                         ') "yoy" '  # end-nested

                         'ON "base"."timestamp"=TIMESTAMPADD(\'year\',1,"yoy"."timestamp") '
                         'ORDER BY "timestamp"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderJoinTests(TestCase):
    maxDiff = None

    def test_dimension_with_join_includes_join_in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
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
                         'ORDER BY "timestamp","district_display"', str(query))

    def test_dimension_with_multiple_joins_includes_joins_ordered__in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes,
                                    slicer.metrics.voters)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.district) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("politician"."timestamp",\'DD\') "timestamp",'
                         '"politician"."district_id" "district",'
                         '"district"."district_name" "district_display",'
                         'SUM("politician"."votes") "votes",'
                         'COUNT("voter"."id") "voters" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" '
                         'ON "politician"."id"="voter"."politician_id" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "timestamp","district","district_display" '
                         'ORDER BY "timestamp","district_display"', str(query))

    def test_dimension_with_recursive_join_joins_all_join_tables(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
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
                         'ORDER BY "timestamp","state_display"', str(query))

    def test_metric_with_join_includes_join_in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.voters)) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         '"politician"."political_party" "political_party",'
                         'COUNT("voter"."id") "voters" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" '
                         'ON "politician"."id"="voter"."politician_id" '
                         'GROUP BY "political_party" '
                         'ORDER BY "political_party"', str(query))

    def test_dimension_filter_with_join_on_display_definition_does_not_include_join_in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.district.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'WHERE "district_id" IN (1)', str(query))

    def test_dimension_filter_display_field_with_join_includes_join_in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.district.isin(['District 4'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'WHERE "district"."district_name" IN (\'District 4\')', str(query))

    def test_dimension_filter_with_recursive_join_includes_join_in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.state.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("politician"."votes") "votes" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'WHERE "district"."state_id" IN (1)', str(query))

    def test_dimension_filter_with_deep_recursive_join_includes_joins_in_query(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
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
                         'WHERE "deep"."id" IN (1)', str(query))


class QueryBuilderOrderTests(TestCase):
    maxDiff = None

    def test_build_query_order_by_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp"', str(query))

    def test_build_query_order_by_dimension_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.candidate) \
            .orderby(slicer.dimensions.candidate_display) \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "candidate","candidate_display" '
                         'ORDER BY "candidate_display"', str(query))

    def test_build_query_order_by_dimension_asc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.asc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp" ASC', str(query))

    def test_build_query_order_by_dimension_desc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.desc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp" DESC', str(query))

    def test_build_query_order_by_metric(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "votes"', str(query))

    def test_build_query_order_by_metric_asc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes, orientation=Order.asc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "votes" ASC', str(query))

    def test_build_query_order_by_metric_desc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes, orientation=Order.desc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "votes" DESC', str(query))

    def test_build_query_order_by_multiple_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp, slicer.dimensions.candidate) \
            .orderby(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display" '
                         'ORDER BY "timestamp","candidate"', str(query))

    def test_build_query_order_by_multiple_dimensions_with_different_orientations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp, slicer.dimensions.candidate) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.desc) \
            .orderby(slicer.dimensions.candidate, orientation=Order.asc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp","candidate","candidate_display" '
                         'ORDER BY "timestamp" DESC,"candidate" ASC', str(query))

    def test_build_query_order_by_metrics_and_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp","votes"', str(query))

    def test_build_query_order_by_metrics_and_dimensions_with_different_orientations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.asc) \
            .orderby(slicer.metrics.votes, orientation=Order.desc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "timestamp",'
                         'SUM("votes") "votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "timestamp" '
                         'ORDER BY "timestamp" ASC,"votes" DESC', str(query))


@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuildPaginationTests(TestCase):
    def test_set_limit(self, mock_fetch_data: Mock):
        slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .fetch(limit=20)

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                'TRUNC("timestamp",\'DD\') "timestamp",'
                                                'SUM("votes") "votes" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "timestamp" '
                                                'ORDER BY "timestamp" LIMIT 20',
                                                dimensions=DimensionMatcher(slicer.dimensions.timestamp))

    def test_set_offset(self, mock_fetch_data: Mock):
        slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .fetch(offset=20)

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                'TRUNC("timestamp",\'DD\') "timestamp",'
                                                'SUM("votes") "votes" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "timestamp" '
                                                'ORDER BY "timestamp" '
                                                'OFFSET 20',
                                                dimensions=DimensionMatcher(slicer.dimensions.timestamp))

    def test_set_limit_and_offset(self, mock_fetch_data: Mock):
        slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .fetch(limit=20, offset=20)

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                'TRUNC("timestamp",\'DD\') "timestamp",'
                                                'SUM("votes") "votes" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "timestamp" '
                                                'ORDER BY "timestamp" '
                                                'LIMIT 20 '
                                                'OFFSET 20',
                                                dimensions=DimensionMatcher(slicer.dimensions.timestamp))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderValidationTests(TestCase):
    maxDiff = None

    def test_highcharts_requires_at_least_one_axis(self):
        with self.assertRaises(MetricRequiredException):
            slicer.data \
                .widget(f.HighCharts()) \
                .dimension(slicer.dimensions.timestamp) \
                .query

    def test_datatablesjs_requires_at_least_one_metric(self):
        with self.assertRaises(TypeError):
            slicer.data \
                .widget(f.DataTablesJS())


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuilderRenderTests(TestCase):
    def test_pass_slicer_database_as_arg(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        slicer.data \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(slicer.database,
                                                ANY,
                                                dimensions=ANY)

    def test_pass_query_from_builder_as_arg(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        slicer.data \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT SUM("votes") "votes" '
                                                'FROM "politics"."politician"',
                                                dimensions=ANY)

    def test_builder_dimensions_as_arg_with_zero_dimensions(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        slicer.data \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, dimensions=[])

    def test_builder_dimensions_as_arg_with_one_dimension(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        dimensions = [slicer.dimensions.state]

        slicer.data \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, dimensions=DimensionMatcher(*dimensions))

    def test_builder_dimensions_as_arg_with_multiple_dimensions(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        dimensions = slicer.dimensions.timestamp, slicer.dimensions.state, slicer.dimensions.political_party

        slicer.data \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, dimensions=DimensionMatcher(*dimensions))

    def test_call_transform_on_widget(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_widget.transform.assert_called_once_with(mock_fetch_data.return_value,
                                                      slicer,
                                                      DimensionMatcher(slicer.dimensions.timestamp),
                                                      [])

    def test_returns_results_from_widget_transform(self, mock_fetch_data: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        result = slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        self.assertListEqual(result, [mock_widget.transform.return_value])

    def test_operations_evaluated(self, mock_fetch_data: Mock):
        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc

        mock_widget = f.Widget(mock_operation)
        mock_widget.transform = Mock()

        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_operation.apply.assert_called_once_with(mock_df)

    def test_operations_results_stored_in_data_frame(self, mock_fetch_data: Mock):
        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc

        mock_widget = f.Widget(mock_operation)
        mock_widget.transform = Mock()

        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        self.assertIn(mock_operation.key, mock_df)
        self.assertEqual(mock_df[mock_operation.key], mock_operation.apply.return_value)
