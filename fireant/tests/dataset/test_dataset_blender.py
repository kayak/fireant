from unittest import TestCase

from fireant import Field
from .mocks import (
    mock_dataset,
    mock_dataset_blender,
    mock_spend_dataset,
)


class BlendedSlicerContainerTests(TestCase):
    def test_access_primary_dataset_metric_via_attr(self):
        votes = mock_dataset_blender.fields.votes
        self.assertIsInstance(votes, Field)
        self.assertEqual(votes.alias, 'votes')

    def test_access_secondary_dataset_metric_via_item(self):
        candidate_spend = mock_dataset_blender.fields['candidate-spend']
        self.assertIsInstance(candidate_spend, Field)
        self.assertEqual(candidate_spend.alias, 'candidate-spend')

    def test_access_blended_metric_via_item(self):
        average_candidate_spend_per_voters = mock_dataset_blender.fields['average-candidate-spend-per-voters']
        self.assertIsInstance(average_candidate_spend_per_voters, Field)
        self.assertEqual(average_candidate_spend_per_voters.alias, 'average-candidate-spend-per-voters')

    def test_access_dimension_via_attr(self):
        timestamp = mock_dataset_blender.fields.timestamp
        self.assertIsInstance(timestamp, Field)
        self.assertEqual(timestamp.alias, 'timestamp')

    def test_iter_fields(self):
        field_aliases = {field.alias for field in mock_dataset_blender.fields}
        self.assertSetEqual(field_aliases, {'votes', 'wins', 'voters', 'turnout', 'wins_with_style',
                                            'timestamp', 'timestamp2', 'join_timestamp',
                                            'political_party',
                                            'candidate-id', 'candidate-name', 'candidate-spend',
                                            'average-candidate-spend-per-voters', 'average-candidate-spend-per-wins',
                                            'election-id', 'election-year',
                                            'district-id', 'district-name',
                                            'state', 'winner', 'deepjoin'})

    def test_has_a_field_mapping_entry_for_the_primary_dataset(self):
        self.assertIn(mock_dataset.table, mock_dataset_blender.field_mapping)

    def test_has_a_field_mapping_entry_for_the_only_secondary_dataset(self):
        self.assertIn(mock_spend_dataset.table, mock_dataset_blender.field_mapping)

    def test_maps_all_fields_from_the_primary_dataset_into_themselves(self):
        self.assertDictEqual(
            mock_dataset_blender.field_mapping[mock_dataset.table],
            {field: field for field in mock_dataset.fields}
        )

    def test_maps_all_fields_across_secondary_datasets_that_have_the_same_alias_when_a_mapping_is_not_provided(self):
        self.assertDictEqual(
            mock_dataset_blender.field_mapping[mock_spend_dataset.table],
            {
                mock_dataset.fields['timestamp']: mock_spend_dataset.fields['timestamp'],
                mock_dataset.fields['candidate-id']: mock_spend_dataset.fields['candidate-id'],
                mock_dataset.fields['election-year']: mock_spend_dataset.fields['election-year'],
            }
        )
