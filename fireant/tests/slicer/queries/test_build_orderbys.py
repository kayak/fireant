from unittest import TestCase

from pypika import Order

import fireant as f
from ..mocks import slicer


class QueryBuilderOrderTests(TestCase):
    maxDiff = None

    def test_build_query_order_by_dimension(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp"', str(query))

    def test_build_query_order_by_dimension_display(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.candidate) \
            .orderby(slicer.dimensions.candidate_display) \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$candidate","$d$candidate_display" '
                         'ORDER BY "$d$candidate_display"', str(query))

    def test_build_query_order_by_dimension_asc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.asc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp" ASC', str(query))

    def test_build_query_order_by_dimension_desc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.desc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp" DESC', str(query))

    def test_build_query_order_by_metric(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$m$votes"', str(query))

    def test_build_query_order_by_metric_asc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes, orientation=Order.asc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$m$votes" ASC', str(query))

    def test_build_query_order_by_metric_desc(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes, orientation=Order.desc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$m$votes" DESC', str(query))

    def test_build_query_order_by_multiple_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp, slicer.dimensions.candidate) \
            .orderby(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.candidate) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                         'ORDER BY "$d$timestamp","$d$candidate"', str(query))

    def test_build_query_order_by_multiple_dimensions_with_different_orientations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp, slicer.dimensions.candidate) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.desc) \
            .orderby(slicer.dimensions.candidate, orientation=Order.asc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp","$d$candidate","$d$candidate_display" '
                         'ORDER BY "$d$timestamp" DESC,"$d$candidate" ASC', str(query))

    def test_build_query_order_by_metrics_and_dimensions(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp) \
            .orderby(slicer.metrics.votes) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp","$m$votes"', str(query))

    def test_build_query_order_by_metrics_and_dimensions_with_different_orientations(self):
        query = slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .orderby(slicer.dimensions.timestamp, orientation=Order.asc) \
            .orderby(slicer.metrics.votes, orientation=Order.desc) \
            .query

        self.assertEqual('SELECT '
                         'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                         'SUM("votes") "$m$votes" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$timestamp" '
                         'ORDER BY "$d$timestamp" ASC,"$m$votes" DESC', str(query))
