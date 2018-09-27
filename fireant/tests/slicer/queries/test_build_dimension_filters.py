from unittest import TestCase

from datetime import date

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
                         'WHERE LOWER("political_party") LIKE LOWER(\'Rep%\')', str(query))

    def test_build_query_with_filter_not_like_categorical_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.not_like('Rep%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT LOWER("political_party") LIKE LOWER(\'Rep%\')', str(query))

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
            .filter(slicer.dimensions.candidate.display.isin(['Donald Trump'])) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE "candidate_name" IN (\'Donald Trump\')', str(query))

    def test_build_query_with_filter_notin_unique_dim_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.display.notin(['Donald Trump'])) \
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
                         'WHERE LOWER("candidate_name") LIKE LOWER(\'%Trump\')', str(query))

    def test_build_query_with_filter_like_display_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE LOWER("candidate_name") LIKE LOWER(\'%Trump\')', str(query))

    def test_build_query_with_filter_not_like_unique_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.not_like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT LOWER("candidate_name") LIKE LOWER(\'%Trump\')', str(query))

    def test_build_query_with_filter_not_like_display_dim(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.not_like('%Trump')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT LOWER("candidate_name") LIKE LOWER(\'%Trump\')', str(query))

    def test_build_query_with_filter_like_categorical_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.like('Rep%', 'Dem%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE LOWER("political_party") LIKE LOWER(\'Rep%\') '
                         'OR LOWER("political_party") LIKE LOWER(\'Dem%\')', str(query))

    def test_build_query_with_filter_not_like_categorical_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.political_party.not_like('Rep%', 'Dem%')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT (LOWER("political_party") LIKE LOWER(\'Rep%\') '
                         'OR LOWER("political_party") LIKE LOWER(\'Dem%\'))', str(query))

    def test_build_query_with_filter_like_unique_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE LOWER("candidate_name") LIKE LOWER(\'%Trump\') '
                         'OR LOWER("candidate_name") LIKE LOWER(\'%Clinton\')', str(query))

    def test_build_query_with_filter_like_display_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE LOWER("candidate_name") LIKE LOWER(\'%Trump\') '
                         'OR LOWER("candidate_name") LIKE LOWER(\'%Clinton\')', str(query))

    def test_build_query_with_filter_not_like_unique_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate.not_like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT (LOWER("candidate_name") LIKE LOWER(\'%Trump\') '
                         'OR LOWER("candidate_name") LIKE LOWER(\'%Clinton\'))', str(query))

    def test_build_query_with_filter_not_like_display_dim_multiple_patterns(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.dimensions.candidate_display.not_like('%Trump', '%Clinton')) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'WHERE NOT (LOWER("candidate_name") LIKE LOWER(\'%Trump\') '
                         'OR LOWER("candidate_name") LIKE LOWER(\'%Clinton\'))', str(query))

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
