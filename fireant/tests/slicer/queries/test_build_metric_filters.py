from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_metric_filter_eq(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes == 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', str(query))

    def test_build_query_with_metric_filter_eq_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 == slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', str(query))

    def test_build_query_with_metric_filter_ne(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes != 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', str(query))

    def test_build_query_with_metric_filter_ne_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 != slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', str(query))

    def test_build_query_with_metric_filter_gt(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes > 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', str(query))

    def test_build_query_with_metric_filter_gt_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 < slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', str(query))

    def test_build_query_with_metric_filter_gte(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes >= 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', str(query))

    def test_build_query_with_metric_filter_gte_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 <= slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', str(query))

    def test_build_query_with_metric_filter_lt(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes < 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', str(query))

    def test_build_query_with_metric_filter_lt_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 > slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', str(query))

    def test_build_query_with_metric_filter_lte(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes <= 5) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', str(query))

    def test_build_query_with_metric_filter_lte_left(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 >= slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', str(query))
