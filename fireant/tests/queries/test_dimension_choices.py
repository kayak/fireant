from unittest import TestCase
from unittest.mock import (
    ANY,
    MagicMock,
    Mock,
    patch,
)

import pandas as pd

from fireant import DataSet, DataType, Field
from fireant.tests.dataset.matchers import (
    FieldMatcher,
    PypikaQueryMatcher,
)
from fireant.tests.dataset.mocks import (
    mock_dataset,
    mock_hint_dataset,
    politicians_table,
    test_database,
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsChoicesQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_choices_for_field(self):
        query = mock_dataset.fields.political_party.choices.sql[0]

        self.assertEqual(
            "SELECT "
            '"political_party" "$political_party" '
            'FROM "politics"."politician" '
            'GROUP BY "$political_party"',
            str(query),
        )

    def test_query_choices_for_field_with_join(self):
        query = mock_dataset.fields["district-name"].choices.sql[0]

        self.assertEqual(
            "SELECT "
            '"district"."district_name" "$district-name" '
            'FROM "politics"."politician" '
            'FULL OUTER JOIN "locations"."district" '
            'ON "politician"."district_id"="district"."id" '
            'GROUP BY "$district-name"',
            str(query),
        )

    def test_filter_choices(self):
        query = (
            mock_dataset.fields["candidate-name"]
            .choices.filter(mock_dataset.fields.political_party.isin(["d", "r"]))
            .sql[0]
        )

        self.assertEqual(
            "SELECT "
            '"candidate_name" "$candidate-name" '
            'FROM "politics"."politician" '
            "WHERE \"political_party\" IN ('d','r') "
            'GROUP BY "$candidate-name"',
            str(query),
        )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsChoicesQueryBuilderWithHintTableTests(TestCase):
    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    def test_query_choices_for_dataset_with_hint_table(self, mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party.choices.fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"political_party" "$political_party" '
                    'FROM "politics"."hints" '
                    'WHERE NOT "political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields.political_party),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[
            ["candidate_name", "varchar(128)"],
            ["candidate_name_display", "varchar(128)"],
        ],
    )
    def test_query_choices_for_field_with_display_hint_table(
        self, mock_get_column_definitions: Mock, mock_fetch_data: Mock
    ):
        mock_hint_dataset.fields.candidate_name.choices.fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"candidate_name" "$candidate_name",'
                    '"candidate_name_display" '
                    '"$candidate_name_display" '
                    'FROM "politics"."hints" '
                    'WHERE NOT "candidate_name" IS NULL '
                    'GROUP BY "$candidate_name",'
                    '"$candidate_name_display" '
                    'ORDER BY "$candidate_name"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields.candidate_name),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[
            ["political_party", "varchar(128)"],
            ["state_id", "varchar(128)"],
        ],
    )
    def test_query_choices_for_filters_from_joins(self, mock_get_column_definitions: Mock, mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party.choices.filter(
            mock_hint_dataset.fields["district-name"].isin(["Manhattan"])
        ).filter(mock_hint_dataset.fields["state"].isin(["Texas"])).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"hints"."political_party" "$political_party" '
                    'FROM "politics"."hints" '
                    'JOIN "locations"."state" ON '
                    '"hints"."state_id"="state"."id" '
                    'WHERE "state"."state_name" IN (\'Texas\') '
                    'AND NOT "hints"."political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields.political_party),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[
            ["political_party", "varchar(128)"],
            ["candidate_name", "varchar(128)"],
        ],
    )
    def test_query_choices_for_filters_from_base(self, mock_get_column_definitions: Mock, mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party.choices.filter(
            mock_hint_dataset.fields.candidate_name.isin(["Bill Clinton"])
        ).filter(mock_hint_dataset.fields["election-year"].isin([1992])).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"political_party" "$political_party" '
                    'FROM "politics"."hints" '
                    "WHERE \"candidate_name\" IN ('Bill Clinton') "
                    'AND NOT "political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields.political_party),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[["political_party", "varchar(128)"]],
    )
    def test_query_choices_for_case_filter(self, mock_get_column_definitions: Mock, mock_fetch_data: Mock):
        mock_hint_dataset.fields.political_party.choices.filter(
            mock_hint_dataset.fields.political_party_case.isin(["Democrat", "Bill Clinton"])
        ).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"political_party" "$political_party" '
                    'FROM "politics"."hints" '
                    'WHERE NOT "political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields.political_party),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[["district_name", "varchar(128)"]],
    )
    def test_query_choices_for_join_dimension(self, mock_get_column_definitions: Mock, mock_fetch_data: Mock):
        mock_hint_dataset.fields["district-name"].choices.fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"district_name" "$district-name" '
                    'FROM "politics"."hints" '
                    'WHERE NOT "district_name" IS NULL '
                    'GROUP BY "$district-name" '
                    'ORDER BY "$district-name"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields["district-name"]),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[
            ["district_name", "varchar(128)"],
            ["candidate_name", "varchar(128)"],
        ],
    )
    def test_query_choices_for_join_dimension_with_filter_from_base(
        self, mock_get_column_definitions: Mock, mock_fetch_data: Mock
    ):
        mock_hint_dataset.fields["district-name"].choices.filter(
            mock_hint_dataset.fields.candidate_name.isin(["Bill Clinton"])
        ).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"district_name" "$district-name" '
                    'FROM "politics"."hints" '
                    "WHERE \"candidate_name\" IN ('Bill Clinton') "
                    'AND NOT "district_name" IS NULL '
                    'GROUP BY "$district-name" '
                    'ORDER BY "$district-name"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields["district-name"]),
        )

    @patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
    @patch.object(
        mock_hint_dataset.database,
        "get_column_definitions",
        return_value=[
            ["district_name", "varchar(128)"],
            ["district_id", "varchar(128)"],
        ],
    )
    def test_query_choices_for_join_dimension_with_filter_from_join(
        self, mock_get_column_definitions: Mock, mock_fetch_data: Mock
    ):
        mock_hint_dataset.fields["district-name"].choices.filter(
            mock_hint_dataset.fields["district-name"].isin(["Manhattan"])
        ).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"hints"."district_name" "$district-name" '
                    'FROM "politics"."hints" '
                    'FULL OUTER JOIN "locations"."district" ON '
                    '"hints"."district_id"="district"."id" '
                    'WHERE "district"."district_name" IN ('
                    "'Manhattan') "
                    'AND NOT "hints"."district_name" IS NULL '
                    'GROUP BY "$district-name" '
                    'ORDER BY "$district-name"'
                )
            ],
            FieldMatcher(mock_hint_dataset.fields["district-name"]),
        )


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(100, MagicMock()))
class DimensionsChoicesFetchTests(TestCase):
    def test_query_choices_for_field(self, mock_fetch_data: Mock):
        mock_dataset.fields.political_party.choices.fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"political_party" "$political_party" '
                    'FROM "politics"."politician" '
                    'WHERE NOT "political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_dataset.fields.political_party),
        )

    def test_envelopes_responses_if_return_additional_metadata_True(self, mock_fetch_data):
        mock_dataset = DataSet(
            table=politicians_table,
            database=test_database,
            return_additional_metadata=True,
            fields=[
                Field(
                    "political_party",
                    label="Party",
                    definition=politicians_table.political_party,
                    data_type=DataType.text,
                    hyperlink_template="http://example.com/{political_party}",
                )
            ],
        )
        df = pd.DataFrame({'political_party': ['a', 'b', 'c']}).set_index('political_party')
        mock_fetch_data.return_value = 100, df

        result = mock_dataset.fields.political_party.choices.fetch()

        self.assertEqual(dict(max_rows_returned=100), result['metadata'])
        self.assertTrue(
            pd.Series(['a', 'b', 'c'], index=['a', 'b', 'c'], name='political_party').equals(result['data'])
        )
