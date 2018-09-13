from datetime import date
from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDatetimeReferenceTests(TestCase):
    maxDiff = None

    def test_single_reference_dod_with_no_dimension_uses_multiple_from_clauses_instead_of_joins(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '

                         'FROM ('
                         'SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician"'
                         ') "$base",('
                         'SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician"'
                         ') "$dod"', str(query))

    def test_single_reference_dod_with_dimension_but_not_reference_dimension_in_query_using_filter(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.political_party) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2000, 1, 1), date(2000, 3, 1))) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$political_party","$dod"."$d$political_party") "$d$political_party",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                         'GROUP BY "$d$political_party"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2000-01-01\' AND \'2000-03-01\' '
                         'GROUP BY "$d$political_party"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$political_party"="$dod"."$d$political_party" '
                         'ORDER BY "$d$political_party"', str(query))

    def test_dimension_with_single_reference_dod(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_single_reference_wow(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.WeekOverWeek(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'week\',1,"$wow"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$wow"."$m$votes" "$m$votes_wow" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$wow" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'week\',1,"$wow"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_single_reference_mom(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.MonthOverMonth(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'month\',1,"$mom"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$mom"."$m$votes" "$m$votes_mom" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$mom" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'month\',1,"$mom"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_single_reference_qoq(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.QuarterOverQuarter(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'quarter\',1,"$qoq"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$qoq"."$m$votes" "$m$votes_qoq" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$qoq" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'quarter\',1,"$qoq"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_single_reference_yoy(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$yoy"."$m$votes" "$m$votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_single_reference_as_a_delta(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp, delta=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$base"."$m$votes"-"$dod"."$m$votes" "$m$votes_dod_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_single_reference_as_a_delta_percentage(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '("$base"."$m$votes"-"$dod"."$m$votes")*100/NULLIF("$dod"."$m$votes",'
                         '0) "$m$votes_dod_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_reference_on_dimension_with_weekly_interval(self):
        weekly_timestamp = slicer.dimensions.timestamp(f.weekly)
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(weekly_timestamp) \
            .reference(f.DayOverDay(weekly_timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_reference_on_dimension_with_weekly_interval_no_interval_on_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_reference_on_dimension_with_monthly_interval(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.monthly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'MM\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'MM\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_reference_on_dimension_with_quarterly_interval(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.quarterly)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Q\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Q\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_reference_on_dimension_with_annual_interval(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.annually)) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Y\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'Y\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_dimension_with_multiple_references(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '

                         'COALESCE('
                         '"$base"."$d$timestamp",'
                         'TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp"),'
                         'TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")'
                         ') "$d$timestamp",'

                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod",'
                         '("$base"."$m$votes"-"$yoy"."$m$votes")*100/NULLIF("$yoy"."$m$votes",'
                         '0) "$m$votes_yoy_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_reference_joins_nested_query_on_dimensions(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.political_party) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         'COALESCE("$base"."$d$political_party","$yoy"."$d$political_party") "$d$political_party",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$yoy"."$m$votes" "$m$votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$political_party"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"political_party" "$d$political_party",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$political_party"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'AND "$base"."$d$political_party"="$yoy"."$d$political_party" '
                         'ORDER BY "$d$timestamp","$d$political_party"', str(query))

    def test_reference_with_unique_dimension_includes_display_definition(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .dimension(slicer.dimensions.candidate) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         'COALESCE("$base"."$d$candidate","$yoy"."$d$candidate") "$d$candidate",'
                         'COALESCE("$base"."$d$candidate_display","$yoy"."$d$candidate_display") '
                         '"$d$candidate_display",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$yoy"."$m$votes" "$m$votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'AND "$base"."$d$candidate"="$yoy"."$d$candidate" '
                         'ORDER BY "$d$timestamp","$d$candidate_display"', str(query))

    def test_adjust_reference_dimension_filters_in_reference_query(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_adjust_reference_dimension_filters_in_reference_query_with_multiple_filters(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp
                    .between(date(2018, 1, 1), date(2018, 1, 31))) \
            .filter(slicer.dimensions.political_party
                    .isin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'AND "political_party" IN (\'d\') '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'day\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'AND "political_party" IN (\'d\') '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$yoy"."$m$votes" "$m$votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_delta_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$base"."$m$votes"-"$yoy"."$m$votes" "$m$votes_yoy_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_delta_percent_reference(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '("$base"."$m$votes"-"$yoy"."$m$votes")*100/NULLIF("$yoy"."$m$votes",'
                         '0) "$m$votes_yoy_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_adapt_dow_for_leap_year_for_yoy_reference_with_date_filter(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp(f.weekly)) \
            .reference(f.YearOverYear(slicer.dimensions.timestamp)) \
            .filter(slicer.dimensions.timestamp.between(date(2018, 1, 1), date(2018, 1, 31))) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$yoy"."$m$votes" "$m$votes_yoy" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'IW\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TIMESTAMPADD(\'year\',-1,TRUNC(TIMESTAMPADD(\'year\',1,"timestamp"),\'IW\')) "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE TIMESTAMPADD(\'year\',1,"timestamp") BETWEEN \'2018-01-01\' AND \'2018-01-31\' '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_adding_duplicate_reference_does_not_join_more_queries(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp, delta=True),
                       f.DayOverDay(slicer.dimensions.timestamp, delta_percent=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE("$base"."$d$timestamp",TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp")) '
                         '"$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod",'
                         '"$base"."$m$votes"-"$dod"."$m$votes" "$m$votes_dod_delta",'
                         '("$base"."$m$votes"-"$dod"."$m$votes")*100/NULLIF("$dod"."$m$votes",'
                         '0) "$m$votes_dod_delta_percent" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_use_same_nested_query_for_joining_references_with_same_period_and_dimension_with_different_periods(self):
        query = slicer.data \
            .widget(f.HighCharts()
                    .axis(f.HighCharts.LineSeries(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .reference(f.DayOverDay(slicer.dimensions.timestamp),
                       f.DayOverDay(slicer.dimensions.timestamp, delta=True),
                       f.YearOverYear(slicer.dimensions.timestamp),
                       f.YearOverYear(slicer.dimensions.timestamp, delta=True)) \
            .query

        self.assertEqual('SELECT '
                         'COALESCE('
                         '"$base"."$d$timestamp",'
                         'TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp"),'
                         'TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp")'
                         ') "$d$timestamp",'
                         '"$base"."$m$votes" "$m$votes",'
                         '"$dod"."$m$votes" "$m$votes_dod",'
                         '"$base"."$m$votes"-"$dod"."$m$votes" "$m$votes_dod_delta",'
                         '"$yoy"."$m$votes" "$m$votes_yoy",'
                         '"$base"."$m$votes"-"$yoy"."$m$votes" "$m$votes_yoy_delta" '
                         'FROM '

                         '('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$base" '  # end-nested

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$dod" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'day\',1,"$dod"."$d$timestamp") '

                         'FULL OUTER JOIN ('  # nested
                         'SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp"'
                         ') "$yoy" '  # end-nested

                         'ON "$base"."$d$timestamp"=TIMESTAMPADD(\'year\',1,"$yoy"."$d$timestamp") '
                         'ORDER BY "$d$timestamp"', str(query))
