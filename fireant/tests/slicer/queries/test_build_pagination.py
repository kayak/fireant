from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

import fireant as f
from ..matchers import (
    DimensionMatcher,
)
from ..mocks import slicer


@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuildPaginationTests(TestCase):
    def test_set_limit(self, mock_fetch_data: Mock):
        slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .limit(20) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                                                'SUM("votes") "$m$votes" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "$d$timestamp" '
                                                'ORDER BY "$d$timestamp" LIMIT 20',
                                                dimensions=DimensionMatcher(slicer.dimensions.timestamp))

    def test_set_offset(self, mock_fetch_data: Mock):
        slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .offset(20) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                                                'SUM("votes") "$m$votes" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "$d$timestamp" '
                                                'ORDER BY "$d$timestamp" '
                                                'OFFSET 20',
                                                dimensions=DimensionMatcher(slicer.dimensions.timestamp))

    def test_set_limit_and_offset(self, mock_fetch_data: Mock):
        slicer.data \
            .widget(f.DataTablesJS(slicer.metrics.votes)) \
            .dimension(slicer.dimensions.timestamp) \
            .limit(20) \
            .offset(30) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                'SELECT '
                                                'TRUNC("timestamp",\'DD\') "$d$timestamp",'
                                                'SUM("votes") "$m$votes" '
                                                'FROM "politics"."politician" '
                                                'GROUP BY "$d$timestamp" '
                                                'ORDER BY "$d$timestamp" '
                                                'LIMIT 20 '
                                                'OFFSET 30',
                                                dimensions=DimensionMatcher(slicer.dimensions.timestamp))
