from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from ..matchers import (
    DimensionMatcher,
    PypikaQueryMatcher,
)
from ..mocks import slicer


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsChoicesQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_choices_for_cat_dimension(self):
        query = slicer.dimensions.political_party \
            .choices \
            .queries[0]

        self.assertEqual('SELECT '
                         '"political_party" "$d$political_party" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$political_party"', str(query))

    def test_query_choices_for_uni_dimension(self):
        query = slicer.dimensions.candidate \
            .choices \
            .queries[0]

        self.assertEqual('SELECT '
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$d$candidate","$d$candidate_display"', str(query))

    def test_query_choices_for_uni_dimension_with_join(self):
        query = slicer.dimensions.district \
            .choices \
            .queries[0]

        self.assertEqual('SELECT '
                         '"politician"."district_id" "$d$district",'
                         '"district"."district_name" "$d$district_display" '
                         'FROM "politics"."politician" '
                         'OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "$d$district","$d$district_display"', str(query))

    def test_filter_choices(self):
        query = slicer.dimensions.candidate \
            .choices \
            .filter(slicer.dimensions.political_party.isin(['d', 'r'])) \
            .queries[0]

        self.assertEqual('SELECT '
                         '"candidate_id" "$d$candidate",'
                         '"candidate_name" "$d$candidate_display" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\',\'r\') '
                         'GROUP BY "$d$candidate","$d$candidate_display"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.slicer.queries.builder.fetch_data')
class DimensionsChoicesFetchTests(TestCase):
    def test_query_choices_for_cat_dimension(self, mock_fetch_data: Mock):
        slicer.dimensions.political_party \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"political_party" "$d$political_party" '
                                                                    'FROM "politics"."politician" '
                                                                    'GROUP BY "$d$political_party" '
                                                                    'ORDER BY "$d$political_party"')],
                                                DimensionMatcher(slicer.dimensions.political_party))

    def test_query_choices_for_uni_dimension(self, mock_fetch_data: Mock):
        slicer.dimensions.candidate \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"candidate_id" "$d$candidate",'
                                                                    '"candidate_name" "$d$candidate_display" '
                                                                    'FROM "politics"."politician" '
                                                                    'GROUP BY "$d$candidate","$d$candidate_display" '
                                                                    'ORDER BY "$d$candidate_display"')],
                                                DimensionMatcher(slicer.dimensions.candidate))
