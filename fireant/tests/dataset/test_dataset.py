from unittest import TestCase

from fireant import Field
from .mocks import mock_dataset


class SlicerContainerTests(TestCase):
    def test_access_metric_via_attr(self):
        votes = mock_dataset.fields.votes
        self.assertIsInstance(votes, Field)
        self.assertEqual(votes.alias, 'votes')

    def test_access_metric_via_item(self):
        votes = mock_dataset.fields['votes']
        self.assertIsInstance(votes, Field)
        self.assertEqual(votes.alias, 'votes')

    def test_access_dimension_via_attr(self):
        timestamp = mock_dataset.fields.timestamp
        self.assertIsInstance(timestamp, Field)
        self.assertEqual(timestamp.alias, 'timestamp')

    def test_access_dimension_via_item(self):
        timestamp = mock_dataset.fields['timestamp']
        self.assertIsInstance(timestamp, Field)
        self.assertEqual(timestamp.alias, 'timestamp')

    def test_iter_fields(self):
        field_aliases = {field.alias for field in mock_dataset.fields}
        self.assertSetEqual(
            field_aliases,
            {
                'votes',
                'wins',
                'voters',
                'turnout',
                'wins_with_style',
                'timestamp',
                'timestamp2',
                'join_timestamp',
                'political_party',
                'candidate-id',
                'candidate-name',
                'election-id',
                'election-year',
                'district-id',
                'district-name',
                'state',
                'winner',
                'deepjoin',
            },
        )
