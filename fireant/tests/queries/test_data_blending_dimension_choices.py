from unittest.case import TestCase
from unittest.mock import (
    patch,
    Mock,
    ANY,
)

from fireant import DataSetException
from fireant.tests.dataset.matchers import (
    PypikaQueryMatcher,
    FieldMatcher,
)
from fireant.tests.dataset.mocks import mock_dataset_blender


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data")
class BlenderDimensionsChoicesFetchTests(TestCase):
    def test_query_choices_for_primary_field_in_dataset_blender(
        self, mock_fetch_data: Mock
    ):
        mock_dataset_blender.fields.political_party.choices.filter(
            mock_dataset_blender.fields.political_party.isin(["d", "r"])
        ).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    'SELECT "political_party" "$political_party" '
                    'FROM "politics"."politician" '
                    "WHERE \"political_party\" IN ('d','r') "
                    'AND NOT "political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_dataset_blender.fields.political_party),
        )

    def test_query_choices_for_secondary_field_in_dataset_blender(
        self, mock_fetch_data: Mock
    ):
        mock_dataset_blender.fields.special.choices.filter(
            mock_dataset_blender.fields.special.isin(["something"])
        ).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    'SELECT "special" "$special" '
                    'FROM "politics"."politician_spend" '
                    "WHERE \"special\" IN ('something') "
                    'AND NOT "special" IS NULL '
                    'GROUP BY "$special" '
                    'ORDER BY "$special"'
                )
            ],
            FieldMatcher(mock_dataset_blender.fields.special),
        )

    def test_query_choices_with_fields_from_different_datasets_raises_exception(
        self, mock_fetch_data: Mock
    ):
        with self.assertRaises(DataSetException):
            mock_dataset_blender.fields.political_party.choices.filter(
                mock_dataset_blender.fields.state.isin(["Texas"])
            ).fetch()
