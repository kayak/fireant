from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderMetricFilterTests(TestCase):
    maxDiff = None

    def test_build_query_with_metric_filter_eq(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes == 5) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', str(queries[0]))

    def test_build_query_with_metric_filter_eq_left(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 == slicer.metrics.votes) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")=5', str(queries[0]))

    def test_build_query_with_metric_filter_ne(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes != 5) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', str(queries[0]))

    def test_build_query_with_metric_filter_ne_left(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 != slicer.metrics.votes) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<>5', str(queries[0]))

    def test_build_query_with_metric_filter_gt(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes > 5) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', str(queries[0]))

    def test_build_query_with_metric_filter_gt_left(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 < slicer.metrics.votes) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>5', str(queries[0]))

    def test_build_query_with_metric_filter_gte(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes >= 5) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', str(queries[0]))

    def test_build_query_with_metric_filter_gte_left(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 <= slicer.metrics.votes) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")>=5', str(queries[0]))

    def test_build_query_with_metric_filter_lt(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes < 5) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', str(queries[0]))

    def test_build_query_with_metric_filter_lt_left(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 > slicer.metrics.votes) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<5', str(queries[0]))

    def test_build_query_with_metric_filter_lte(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(slicer.metrics.votes <= 5) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', str(queries[0]))

    def test_build_query_with_metric_filter_lte_left(self):
        queries = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .filter(5 >= slicer.metrics.votes) \
            .queries

        self.assertEqual(len(queries), 1)

        self.assertEqual('SELECT '
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'HAVING SUM("votes")<=5', str(queries[0]))
