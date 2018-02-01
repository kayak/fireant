from unittest import TestCase

from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsOptionsQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_options_for_cat_dimension(self):
        query = slicer.dimensions.political_party \
            .options \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "political_party" '
                         'FROM "politics"."politician" '
                         'GROUP BY "political_party"', str(query))

    def test_query_options_for_uni_dimension(self):
        query = slicer.dimensions.candidate \
            .options \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display" '
                         'FROM "politics"."politician" '
                         'GROUP BY "candidate","candidate_display"', str(query))

    def test_query_options_for_uni_dimension_with_join(self):
        query = slicer.dimensions.district \
            .options \
            .query

        self.assertEqual('SELECT '
                         '"politician"."district_id" "district",'
                         '"district"."district_name" "district_display" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "district","district_display"', str(query))

    def test_no_options_attr_for_datetime_dimension(self):
        with self.assertRaises(AttributeError):
            slicer.dimensions.timestamp.options

    def test_no_options_attr_for_boolean_dimension(self):
        with self.assertRaises(AttributeError):
            slicer.dimensions.winner.options

    def test_filter_options(self):
        query = slicer.dimensions.candidate \
            .options \
            .filter(slicer.dimensions.political_party.isin(['d', 'r'])) \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "candidate",'
                         '"candidate_name" "candidate_display" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\',\'r\') '
                         'GROUP BY "candidate","candidate_display"', str(query))
