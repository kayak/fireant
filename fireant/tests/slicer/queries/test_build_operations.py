from unittest import TestCase

import fireant as f
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class QueryBuilderOperationTests(TestCase):
    maxDiff = None

    def test_build_query_with_cumsum_operation(self):
        query = slicer.data \
            .widget(f.DataTablesJS(f.CumSum(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_cummean_operation(self):
        query = slicer.data \
            .widget(f.DataTablesJS(f.CumMean(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_cumprod_operation(self):
        query = slicer.data \
            .widget(f.DataTablesJS(f.CumProd(slicer.metrics.votes))) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_with_rollingmean_operation(self):
        query = slicer.data \
            .widget(f.DataTablesJS(f.RollingMean(slicer.metrics.votes, 3, 3))) \
            .dimension(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))
