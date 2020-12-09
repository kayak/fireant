from unittest.case import TestCase
from unittest.mock import (
    ANY,
    MagicMock,
    Mock,
    patch,
)

from fireant.tests.dataset.matchers import (
    FieldMatcher,
    PypikaQueryMatcher,
)
from fireant.tests.dataset.mocks import mock_dataset_blender


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch("fireant.queries.builder.dimension_choices_query_builder.fetch_data", return_value=(1, MagicMock()))
class BlenderDimensionsChoicesFetchTests(TestCase):
    def test_query_choices_for_primary_field_in_dataset_blender(self, mock_fetch_data: Mock):
        d_or_r = mock_dataset_blender.fields.political_party.isin(["d", "r"])
        mock_dataset_blender.fields.political_party.choices.filter(d_or_r).fetch()

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

    def test_query_choices_for_secondary_field_in_dataset_blender(self, mock_fetch_data: Mock):
        something = mock_dataset_blender.fields.special.isin(["something"])
        mock_dataset_blender.fields.special.choices.filter(something).fetch()

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

    def test_silently_omit_filters_on_fields_from_foreign_datasets(self, mock_fetch_data: Mock):
        special = mock_dataset_blender.fields.special.isin(["something"])
        mock_dataset_blender.fields.political_party.choices.filter(special).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [
                PypikaQueryMatcher(
                    'SELECT "political_party" "$political_party" '
                    'FROM "politics"."politician" '
                    'WHERE NOT "political_party" IS NULL '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party"'
                )
            ],
            FieldMatcher(mock_dataset_blender.fields.political_party),
        )
