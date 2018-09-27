from datetime import date
from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTests(TestCase):
    maxDiff = None

    def test_build_query_with_datetime_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_datetime_dimension_hourly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.hourly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'HH\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_datetime_dimension_daily(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.daily)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_datetime_dimension_weekly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_datetime_dimension_monthly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'MM\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_datetime_dimension_quarterly(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Q\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_datetime_dimension_annually(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'Y\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_boolean_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.winner) \
            .query

        self.assertEqual('SELECT '
                         '"is_winner" "$d$winner",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$winner" '
                         'ORDER BY "$d$winner"', str(query))

    def test_build_query_with_categorical_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$political_party" '
                         'ORDER BY "$d$political_party"', str(query))

    def test_build_query_with_unique_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.election) \
            .query

        self.assertEqual('SELECT '
                         '"election_id" "$d$election",'
                         '"election_year" "$d$election_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$election","$d$election_display" '
                         'ORDER BY "$d$election_display"', str(query))

    def test_build_query_with_multiple_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                         'ORDER BY "$d$timestamp","$d$candidate_display"', str(query))

    def test_build_query_with_multiple_dimensions_and_visualizations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes, slicer.metrics.wins)) \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))
                    .axis(f.HighCharts.LineSeries(slicer.metrics.wins))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes",'
                         'SUM("is_winner") "$m$wins" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$political_party" '
                         'ORDER BY "$d$timestamp","$d$political_party"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionTotalsTests(TestCase):
    maxDiff = None

    def test_build_query_with_totals_cat_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .query

        self.assertEqual('(SELECT '
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'NULL "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician") '

                         'ORDER BY "$d$political_party"', str(query))

    def test_build_query_with_totals_uni_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.candidate.rollup()) \
            .query

        self.assertEqual('(SELECT '
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$candidate","$d$candidate_display") '

                         'UNION ALL '

                         '(SELECT '
                         'NULL "$d$candidate",'
                         'NULL "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician") '

                         'ORDER BY "$d$candidate_display"', str(query))

    def test_build_query_with_totals_on_dimension_and_subsequent_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup(),
                       slicer.dimensions.political_party) \
            .query

        self.assertEqual('(SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display","$d$political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'NULL "$d$candidate",'
                         'NULL "$d$candidate_display",'
                         'NULL "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp") '
                         'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(query))

    def test_build_query_with_totals_on_multiple_dimensions_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp,
                       slicer.dimensions.candidate.rollup(),
                       slicer.dimensions.political_party.rollup()) \
            .query

        self.assertEqual('(SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display","$d$political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'NULL "$d$candidate",'
                         'NULL "$d$candidate_display",'
                         'NULL "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp") '

                         'UNION ALL '

                         '(SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'NULL "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display") '

                         'ORDER BY "$d$timestamp","$d$candidate_display","$d$political_party"', str(query))

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
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         'COALESCE("$base"."$d$political_party","$dod"."$d$political_party") "$d$political_party",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM ('

                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$political_party"'
                         ') "$base" '

                         'FULL OUTER JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$political_party"'
                         ') "$dod" '
                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'AND "$base"."$d$political_party"="$dod"."$d$political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         'NULL "$d$political_party",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '

                         'FULL OUTER JOIN ('
                         'SELECT TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '
                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         'ORDER BY "$d$timestamp","$d$political_party"', str(query))

    def test_build_query_with_totals_cat_dimension_with_references_and_date_filters(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party.rollup()) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2018, 1, 1), date(2019, 1, 1))) \
            .query

        self.assertEqual('(SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         'COALESCE("$base"."$d$political_party","$dod"."$d$political_party") "$d$political_party",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM ('

                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "$d$timestamp","$d$political_party"'
                         ') "$base" '

                         'FULL OUTER JOIN ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "$d$timestamp","$d$political_party"'
                         ') "$dod" '
                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'AND "$base"."$d$political_party"="$dod"."$d$political_party") '

                         'UNION ALL '

                         '(SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         'NULL "$d$political_party",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM ('
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '

                         'FULL OUTER JOIN ('
                         'SELECT TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2019-01-01\' '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '
                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         'ORDER BY "$d$timestamp","$d$political_party"', str(query))
