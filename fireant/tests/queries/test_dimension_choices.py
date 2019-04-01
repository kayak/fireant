from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from fireant.tests.dataset.matchers import (
    FieldMatcher,
    PypikaQueryMatcher,
)
from fireant.tests.dataset.mocks import mock_dataset


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsChoicesQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_choices_for_field(self):
        query = mock_dataset.fields.political_party \
            .choices \
            .sql[0]

        self.assertEqual('SELECT '
                         '"political_party" "$political_party" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$political_party"', str(query))

    def test_query_choices_for_field_with_join(self):
        query = mock_dataset.fields['district-name'] \
            .choices \
            .sql[0]

        self.assertEqual('SELECT '
                         '"district"."district_name" "$district-name" '
                         'FROM "politics"."politician" '
                         'FULL OUTER JOIN "locations"."district" '
                         'ON "politician"."district_id"="district"."id" '
                         'GROUP BY "$district-name"', str(query))

    def test_filter_choices(self):
        query = mock_dataset.fields['candidate-name'] \
            .choices \
            .filter(mock_dataset.fields.political_party.isin(['d', 'r'])) \
            .sql[0]

        self.assertEqual('SELECT '
                         '"candidate_name" "$candidate-name" '
                         'FROM "politics"."politician" '
                         'WHERE "political_party" IN (\'d\',\'r\') '
                         'GROUP BY "$candidate-name"', str(query))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.queries.builder.fetch_data')
class DimensionsChoicesFetchTests(TestCase):
    def test_query_choices_for_field(self, mock_fetch_data: Mock):
        mock_dataset.fields.political_party \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"political_party" "$political_party" '
                                                                    'FROM "politics"."politician" '
                                                                    'GROUP BY "$political_party" '
                                                                    'ORDER BY "$political_party"')],
                                                FieldMatcher(mock_dataset.fields.political_party))
