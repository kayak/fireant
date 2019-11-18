from unittest import TestCase

from fireant import Field
from .mocks import mock_dataset_blender


class BlendedSlicerContainerTests(TestCase):
    def test_access_primary_dataset_metric_via_attr(self):
        votes = mock_dataset_blender.fields.votes
        self.assertIsInstance(votes, Field)
        self.assertEqual(votes.alias, 'votes')

    def test_access_primary_dataset_metric_via_item(self):
        votes = mock_dataset_blender.fields['votes']
        self.assertIsInstance(votes, Field)
        self.assertEqual(votes.alias, 'votes')

    def test_access_blended_metric_via_item(self):
        average_candidate_spend_per_candidacy = mock_dataset_blender.fields['average-candidate-spend-per-candidacy']
        self.assertIsInstance(average_candidate_spend_per_candidacy, Field)
        self.assertEqual(average_candidate_spend_per_candidacy.alias, 'average-candidate-spend-per-candidacy')

    def test_access_primary_dataset_dimension_via_attr(self):
        timestamp = mock_dataset_blender.fields.timestamp
        self.assertIsInstance(timestamp, Field)
        self.assertEqual(timestamp.alias, 'timestamp')

    def test_access_primary_dataset_dimension_via_item(self):
        timestamp = mock_dataset_blender.fields['timestamp']
        self.assertIsInstance(timestamp, Field)
        self.assertEqual(timestamp.alias, 'timestamp')

    def test_iter_fields(self):
        field_aliases = {field.alias for field in mock_dataset_blender.fields}
        self.assertSetEqual(field_aliases, {'votes', 'wins', 'voters', 'turnout', 'wins_with_style',
                                            'timestamp', 'timestamp2', 'join_timestamp',
                                            'political_party',
                                            'candidate-id', 'candidate-name',
                                            'average-candidate-spend-per-candidacy', 'candidate-spend',
                                            'election-id', 'election-year',
                                            'district-id', 'district-name',
                                            'state', 'winner', 'deepjoin'})
