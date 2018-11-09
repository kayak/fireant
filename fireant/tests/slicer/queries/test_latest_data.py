from unittest import TestCase

from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsLatestQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_single_dimension(self):
        query = slicer.latest(slicer.dimensions.timestamp) \
            .queries[0]

        self.assertEqual('SELECT '
                         'MAX("timestamp") "$d$timestamp" '
                         'FROM "politics"."politician"', str(query))

    def test_query_single_dimension_with_join(self):
        query = slicer.latest(slicer.dimensions.join_timestamp) \
            .queries[0]

        self.assertEqual('SELECT '
                         'MAX("voter"."timestamp") "$d$join_timestamp" '
                         'FROM "politics"."politician" '
                         'JOIN "politics"."voter" ON "politician"."id"="voter"."politician_id"', str(query))

    def test_query_multiple_dimensions(self):
        query = slicer.latest(slicer.dimensions.timestamp, slicer.dimensions.timestamp2) \
            .queries[0]

        self.assertEqual('SELECT '
                         'MAX("timestamp") "$d$timestamp",'
                         'MAX("timestamp2") "$d$timestamp2" '
                         'FROM "politics"."politician"', str(query))
