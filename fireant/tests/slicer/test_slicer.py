from unittest import TestCase

from fireant import (
    Dimension,
    Metric,
)
from fireant.slicer.dimensions import DisplayDimension
from .mocks import slicer


class SlicerContainerTests(TestCase):
    def test_access_metric_via_attr(self):
        votes = slicer.metrics.votes
        self.assertIsInstance(votes, Metric)
        self.assertEqual(votes.key, 'votes')

    def test_access_metric_via_item(self):
        votes = slicer.metrics['votes']
        self.assertIsInstance(votes, Metric)
        self.assertEqual(votes.key, 'votes')

    def test_access_dimension_via_attr(self):
        timestamp = slicer.dimensions.timestamp
        self.assertIsInstance(timestamp, Dimension)
        self.assertEqual(timestamp.key, 'timestamp')

    def test_access_dimension_via_item(self):
        timestamp = slicer.dimensions['timestamp']
        self.assertIsInstance(timestamp, Dimension)
        self.assertEqual(timestamp.key, 'timestamp')

    def test_access_dimension_display_via_attr(self):
        candidate_display = slicer.dimensions.candidate_display
        self.assertIsInstance(candidate_display, DisplayDimension)
        self.assertEqual(candidate_display.key, 'candidate_display')

    def test_access_dimension_display_via_item(self):
        candidate_display = slicer.dimensions['candidate_display']
        self.assertIsInstance(candidate_display, DisplayDimension)
        self.assertEqual(candidate_display.key, 'candidate_display')

    def test_iter_metrics(self):
        metric_keys = [metric.key for metric in slicer.metrics]
        self.assertListEqual(metric_keys, ['votes', 'wins', 'voters', 'turnout'])

    def test_iter_dimensions(self):
        dimension_keys = [dimension.key for dimension in slicer.dimensions]
        self.assertListEqual(dimension_keys, ['timestamp', 'timestamp2', 'join_timestamp',
                                              'political_party', 'candidate', 'election', 'district',
                                              'state', 'winner', 'deepjoin'])
