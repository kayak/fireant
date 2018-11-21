from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    call,
    patch,
)

import fireant as f
from fireant.utils import (
    format_metric_key,
)
from pypika import (
    Order,
    functions as fn,
)
from ..matchers import (
    DimensionMatcher,
    PypikaQueryMatcher,
)
from ..mocks import (
    ElectionOverElection,
    slicer,
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.slicer.queries.builder.paginate')
@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuilderRenderTests(TestCase):
    def test_pass_slicer_database_as_arg(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        slicer.data \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(slicer.database,
                                                ANY,
                                                ANY,
                                                ANY)

    def test_pass_query_from_builder_as_arg(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        slicer.data \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT SUM("votes") "$m$votes" '
                                                                    'FROM "politics"."politician"')],
                                                ANY,
                                                ANY)

    def test_builder_dimensions_as_arg_with_zero_dimensions(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        slicer.data \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, [], ANY)

    def test_builder_dimensions_as_arg_with_one_dimension(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        dimensions = [slicer.dimensions.state]

        slicer.data \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, DimensionMatcher(*dimensions), ANY)

    def test_builder_dimensions_as_arg_with_multiple_dimensions(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        dimensions = slicer.dimensions.timestamp, slicer.dimensions.state, slicer.dimensions.political_party

        slicer.data \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, DimensionMatcher(*dimensions), ANY)

    def test_call_transform_on_widget(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_widget.transform.assert_called_once_with(mock_paginate.return_value,
                                                      slicer,
                                                      DimensionMatcher(slicer.dimensions.timestamp),
                                                      [])

    def test_returns_results_from_widget_transform(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        result = slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        self.assertListEqual(result, [mock_widget.transform.return_value])


@patch('fireant.slicer.queries.builder.paginate')
@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuilderPaginationTests(TestCase):
    def test_paginate_is_called(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()
        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=None, offset=None, orders=[])

    def test_pagination_applied_with_limit(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .limit(15) \
            .fetch()

        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=15, offset=None, orders=[])

    def test_pagination_applied_with_offset(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .limit(15) \
            .offset(20) \
            .fetch()

        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=15, offset=20, orders=[])

    def test_pagination_applied_with_orders(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(slicer.metrics.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .orderby(slicer.metrics.votes, Order.asc) \
            .fetch()

        votes_definition_with_alias_matcher = PypikaQueryMatcher(fn.Sum(slicer.table.votes).as_('$d$votes'))
        orders = [(votes_definition_with_alias_matcher, Order.asc)]
        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=None, offset=None, orders=orders)


@patch('fireant.slicer.queries.builder.paginate')
@patch('fireant.slicer.queries.builder.fetch_data')
class QueryBuilderOperationsTests(TestCase):
    def test_operations_evaluated(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc
        mock_operation.metrics = []

        mock_widget = f.Widget(mock_operation)
        mock_widget.transform = Mock()

        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_operation.apply.assert_called_once_with(mock_df, None)

    def test_operations_evaluated_for_each_reference(self, mock_fetch_data: Mock, mock_paginate: Mock):
        eoe = ElectionOverElection(slicer.dimensions.timestamp)

        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc
        mock_operation.metrics = []

        mock_widget = f.Widget(mock_operation)
        mock_widget.transform = Mock()

        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .reference(eoe) \
            .widget(mock_widget) \
            .fetch()

        mock_operation.apply.assert_has_calls([
            call(mock_df, None),
            call(mock_df, eoe),
        ])

    def test_operations_results_stored_in_data_frame(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_operation = Mock(name='mock_operation ', spec=f.Operation)
        mock_operation.key, mock_operation.definition = 'mock_operation', slicer.table.abc
        mock_operation.metrics = []

        mock_widget = f.Widget(mock_operation)
        mock_widget.transform = Mock()

        mock_df = {}
        mock_fetch_data.return_value = mock_df

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        slicer.data \
            .dimension(slicer.dimensions.timestamp) \
            .widget(mock_widget) \
            .fetch()

        f_op_key = format_metric_key(mock_operation.key)
        self.assertIn(f_op_key, mock_df)
        self.assertEqual(mock_df[f_op_key], mock_operation.apply.return_value)
