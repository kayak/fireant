from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from ..matchers import DimensionMatcher
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsChoicesQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_choices_for_cat_dimension(self):
        query = slicer.dimensions.political_party \
            .choices \
            .query

        self.assertEqual('SELECT '
                         '"political_party" "$political_party" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$political_party"', str(query))

    def test_query_choices_for_uni_dimension(self):
        query = slicer.dimensions.candidate \
            .choices \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "$candidate",'
                         '"candidate_name" "$candidate_display" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$candidate","$candidate_display"', str(query))

    def test_query_choices_for_uni_dimension_with_join(self):
        query = slicer.dimensions.district \
            .choices \
            .query

        self.assertEqual('SELECT '
                         '"politician"."district_id" "$district",'
                         '"district"."district_name" "$district_display" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "$district","$district_display"', str(query))

    def test_no_choices_attr_for_datetime_dimension(self):
        with self.assertRaises(AttributeError):
            slicer.dimensions.timestamp.choices

    def test_no_choices_attr_for_boolean_dimension(self):
        with self.assertRaises(AttributeError):
            slicer.dimensions.winner.choices

    def test_filter_choices(self):
        query = slicer.dimensions.candidate \
            .choices \
            .filter(slicer.dimensions.political_party.isin(['d', 'r'])) \
            .query

        self.assertEqual('SELECT '
                         '"candidate_id" "$candidate",'
                         '"candidate_name" "$candidate_display" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\',\'r\') '
                         'GROUP BY "$candidate","$candidate_display"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.slicer.queries.builder.fetch_data')
class DimensionsChoicesFetchTests(TestCase):
    def test_query_choices_for_cat_dimension(self, mock_fetch_data: Mock):
        slicer.dimensions.political_party \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                '"political_party" "$political_party" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "$political_party" '
                                                'ORDER BY "$political_party"',
                                                dimensions=DimensionMatcher(slicer.dimensions.political_party))

    def test_query_choices_for_uni_dimension(self, mock_fetch_data: Mock):
        slicer.dimensions.candidate \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                '"candidate_id" "$candidate",'
                                                '"candidate_name" "$candidate_display" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "$candidate","$candidate_display" '
                                                'ORDER BY "$candidate_display"',
                                                dimensions=DimensionMatcher(slicer.dimensions.candidate))
