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
class DimensionsChoicesQueryBuilderWithHintTableTests(TestCase):
    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    def test_query_choices_for_dataset_with_hint_table(self, mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"political_party" "$political_party" '
                                                                    'FROM "politics"."hints" '
                                                                    'GROUP BY "$political_party" '
                                                                    'ORDER BY "$political_party"')],
                                                FieldMatcher(mock_hint_dataset.fields.political_party))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['candidate_name', 'varchar(128)'], ['candidate_name_display', 'varchar(128)']])
    def test_query_choices_for_field_with_display_hint_table(self,
                                                             mock_get_column_definitions: Mock,
                                                             mock_fetch_data: Mock):
        mock_hint_dataset.fields.candidate_name \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"candidate_name" "$candidate_name",'
                                                                    '"candidate_name_display" '
                                                                    '"$candidate_name_display" '
                                                                    'FROM "politics"."hints" '
                                                                    'GROUP BY "$candidate_name",'
                                                                    '"$candidate_name_display" '
                                                                    'ORDER BY "$candidate_name"')],
                                                FieldMatcher(mock_hint_dataset.fields.candidate_name))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['political_party', 'varchar(128)'], ['state_id', 'varchar(128)']])
    def test_query_choices_for_filters_from_joins(self,
                                                  mock_get_column_definitions: Mock,
                                                  mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party \
            .choices \
            .filter(mock_hint_dataset.fields['district-name'].isin(['Manhattan'])) \
            .filter(mock_hint_dataset.fields['state'].isin(['Texas'])) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"hints"."political_party" "$political_party" '
                                                                    'FROM "politics"."hints" '
                                                                    'JOIN "locations"."state" ON '
                                                                    '"hints"."state_id"="state"."id" '
                                                                    'WHERE "state"."state_name" IN (\'Texas\') '
                                                                    'GROUP BY "$political_party" '
                                                                    'ORDER BY "$political_party"')],
                                                FieldMatcher(mock_hint_dataset.fields.political_party))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['political_party', 'varchar(128)'], ['candidate_name', 'varchar(128)']])
    def test_query_choices_for_filters_from_base(self,
                                                 mock_get_column_definitions: Mock,
                                                 mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party \
            .choices \
            .filter(mock_hint_dataset.fields.candidate_name.isin(['Bill Clinton'])) \
            .filter(mock_hint_dataset.fields['election-year'].isin([1992])) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"political_party" "$political_party" '
                                                                    'FROM "politics"."hints" '
                                                                    'WHERE "candidate_name" IN (\'Bill Clinton\') '
                                                                    'GROUP BY "$political_party" '
                                                                    'ORDER BY "$political_party"')],
                                                FieldMatcher(mock_hint_dataset.fields.political_party))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['political_party', 'varchar(128)']])
    def test_query_choices_for_case_filter(self,
                                           mock_get_column_definitions: Mock,
                                           mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party \
            .choices \
            .filter(mock_hint_dataset.fields.political_party_case.isin(['Democrat', 'Bill Clinton'])) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"political_party" "$political_party" '
                                                                    'FROM "politics"."hints" '
                                                                    'GROUP BY "$political_party" '
                                                                    'ORDER BY "$political_party"')],
                                                FieldMatcher(mock_hint_dataset.fields.political_party))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['district_name', 'varchar(128)']])
    def test_query_choices_for_join_dimension(self,
                                              mock_get_column_definitions: Mock,
                                              mock_fetch_data: Mock):
        mock_hint_dataset.fields['district-name'] \
            .choices \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"district_name" "$district-name" '
                                                                    'FROM "politics"."hints" '
                                                                    'GROUP BY "$district-name" '
                                                                    'ORDER BY "$district-name"')],
                                                FieldMatcher(mock_hint_dataset.fields['district-name']))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['district_name', 'varchar(128)'], ['candidate_name', 'varchar(128)']])
    def test_query_choices_for_join_dimension_with_filter_from_base(self,
                                                                    mock_get_column_definitions: Mock,
                                                                    mock_fetch_data: Mock):
        mock_hint_dataset.fields['district-name'] \
            .choices \
            .filter(mock_hint_dataset.fields.candidate_name.isin(['Bill Clinton'])) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"district_name" "$district-name" '
                                                                    'FROM "politics"."hints" '
                                                                    'WHERE "candidate_name" IN (\'Bill Clinton\') '
                                                                    'GROUP BY "$district-name" '
                                                                    'ORDER BY "$district-name"')],
                                                FieldMatcher(mock_hint_dataset.fields['district-name']))

    @patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
    @patch.object(mock_hint_dataset.database, 'get_column_definitions',
                  return_value=[['district_name', 'varchar(128)'], ['district_id', 'varchar(128)']])
    def test_query_choices_for_join_dimension_with_filter_from_join(self,
                                                                    mock_get_column_definitions: Mock,
                                                                    mock_fetch_data: Mock):
        mock_hint_dataset.fields['district-name'] \
            .choices \
            .filter(mock_hint_dataset.fields['district-name'].isin(['Manhattan'])) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT '
                                                                    '"hints"."district_name" "$district-name" '
                                                                    'FROM "politics"."hints" '
                                                                    'FULL OUTER JOIN "locations"."district" ON '
                                                                    '"hints"."district_id"="district"."id" '
                                                                    'WHERE "district"."district_name" IN ('
                                                                    '\'Manhattan\') '
                                                                    'GROUP BY "$district-name" '
                                                                    'ORDER BY "$district-name"')],
                                                FieldMatcher(mock_hint_dataset.fields['district-name']))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.queries.builder.dimension_choices_query_builder.fetch_data')
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
