from datetime import date
from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderDimensionFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_filter_isin_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.isin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\')', str(query))

    def test_build_query_with_filter_notin_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.notin(['d'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" NOT IN (\'d\')', str(query))

    def test_build_query_with_filter_like_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.like('Rep%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" LIKE \'Rep%\'', str(query))

    def test_build_query_with_filter_not_like_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.not_like('Rep%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" NOT LIKE \'Rep%\'', str(query))

    def test_build_query_with_filter_isin_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.isin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" IN (1)', str(query))

    def test_build_query_with_filter_notin_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.notin([1])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_id" NOT IN (1)', str(query))

    def test_build_query_with_filter_isin_unique_dim_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.isin(['Donald Trump'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" IN (\'Donald Trump\')', str(query))

    def test_build_query_with_filter_notin_unique_dim_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.notin(['Donald Trump'], use_display=True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT IN (\'Donald Trump\')', str(query))

    def test_build_query_with_filter_like_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_like_display_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_not_like_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.not_like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_not_like_display_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.not_like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT LIKE \'%Trump\'', str(query))

    def test_build_query_with_filter_like_categorical_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.like('Rep%', 'Dem%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" LIKE \'Rep%\' '
                         'OR "political_party" LIKE \'Dem%\'', str(query))

    def test_build_query_with_filter_not_like_categorical_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.not_like('Rep%', 'Dem%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" NOT LIKE \'Rep%\' '
                         'OR "political_party" NOT LIKE \'Dem%\'', str(query))

    def test_build_query_with_filter_like_pattern_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.pattern.like('a%', 'b%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "pattern" LIKE \'a%\' '
                         'OR "pattern" LIKE \'b%\'', str(query))

    def test_build_query_with_filter_not_like_pattern_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.pattern.not_like('a%', 'b%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "pattern" NOT LIKE \'a%\' '
                         'OR "pattern" NOT LIKE \'b%\'', str(query))

    def test_build_query_with_filter_like_unique_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\' '
                         'OR "candidate_name" LIKE \'%Clinton\'', str(query))

    def test_build_query_with_filter_like_display_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" LIKE \'%Trump\' '
                         'OR "candidate_name" LIKE \'%Clinton\'', str(query))

    def test_build_query_with_filter_not_like_unique_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.not_like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT LIKE \'%Trump\' '
                         'OR "candidate_name" NOT LIKE \'%Clinton\'', str(query))

    def test_build_query_with_filter_not_like_display_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.not_like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" NOT LIKE \'%Trump\' '
                         'OR "candidate_name" NOT LIKE \'%Clinton\'', str(query))

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
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "timestamp" BETWEEN \'2009-01-20\' AND \'2017-01-20\'', str(query))

    def test_build_query_with_filter_boolean_true(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.winner.is_(True)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "is_winner"', str(query))

    def test_build_query_with_filter_boolean_false(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.winner.is_(False)) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT "is_winner"', str(query))
