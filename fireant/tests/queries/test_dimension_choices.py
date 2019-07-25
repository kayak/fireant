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
from fireant.tests.dataset.mocks import (
    mock_dataset,
    mock_hint_dataset,
)


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

    @patch.object(mock_hint_dataset.database, 'get_column_definitions', return_value=['political_party'])
    def test_query_choices_for_field_with_hint_table(self, mock_get_column_definitions):
        query = mock_hint_dataset.fields.political_party \
            .choices \
            .sql[0]

        self.assertEqual('SELECT '
                         '"political_party" "$political_party" '
                         'FROM "politics"."politician_hints" '
                         'GROUP BY "$political_party"', str(query))

    def test_query_choices_for_field_with_empty_hint_table(self):
        query = mock_hint_dataset.fields.political_party \
            .choices \
            .sql[0]

        self.assertEqual('SELECT '
                         '"political_party" "$political_party" '
                         'FROM "politics"."politician" '
                         'GROUP BY "$political_party"', str(query))

    @patch.object(mock_hint_dataset.database, 'get_column_definitions', return_value=['political_party', 'state_name'])
    def test_query_choices_for_case_field_with_complete_hint_table(self, mock_get_column_definition):
        query = mock_hint_dataset.fields.political_party_case \
            .choices \
            .sql[0]

        self.assertEqual('SELECT CASE '
                         'WHEN "political_party"=\'Democrat\' THEN \'Democrat\' '
                         'WHEN "state_name"=\'California\' THEN \'California\' '
                         'ELSE \'\' END '
                         '"$political_party_case" '
                         'FROM "politics"."politician_hints" '
                         'GROUP BY "$political_party_case"', str(query))

    @patch.object(mock_hint_dataset.database, 'get_column_definitions', return_value=['political_party'])
    def test_query_choices_for_case_field_with_incomplete_hint_table(self, mock_get_column_definition):
        query = mock_hint_dataset.fields.political_party_case \
            .choices \
            .sql[0]

        self.assertEqual('SELECT CASE '
                         'WHEN "politician"."political_party"=\'Democrat\' THEN \'Democrat\' '
                         'WHEN "state"."state_name"=\'California\' THEN \'California\' '
                         'ELSE \'\' END '
                         '"$political_party_case" '
                         'FROM "politics"."politician" '
                         'FULL OUTER JOIN "locations"."district" ON "politician"."district_id"="district"."id" '
                         'JOIN "locations"."state" ON "district"."state_id"="state"."id" '
                         'GROUP BY "$political_party_case"', str(query))

    @patch.object(mock_hint_dataset.database, 'get_column_definitions', return_value=['political_party', 'candidate_name'])
    def test_query_choices_for_field_with_hint_table_and_filters(self, mock_get_column_definition):
        query = mock_hint_dataset.fields.political_party \
            .choices \
            .filter(mock_hint_dataset.fields['candidate-name'].isin(['Bill Clinton', 'Bob Dole'])) \
            .filter(mock_hint_dataset.fields['state'].isin(['Texas', 'California'])) \
            .sql[0]

        self.assertEqual('SELECT '
                         '"political_party" "$political_party" '
                         'FROM "politics"."politician_hints" '
                         'WHERE "candidate_name" IN (\'Bill Clinton\',\'Bob Dole\') '
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
