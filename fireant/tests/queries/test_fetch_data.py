from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

import fireant as f
from fireant import Share
from fireant.tests.dataset.matchers import (
    FieldMatcher,
    PypikaQueryMatcher,
)
from fireant.tests.dataset.mocks import mock_dataset
from pypika import (
    Order,
    functions as fn,
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.queries.builder.dataset_query_builder.paginate')
@patch('fireant.queries.builder.dataset_query_builder.fetch_data')
class FindShareDimensionsTests(TestCase):
    def test_find_no_share_dimensions_with_no_share_operation(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = mock_dataset.fields.timestamp, mock_dataset.fields.state, mock_dataset.fields.political_party

        mock_dataset.query \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, [], ANY)

    def test_find_share_dimensions_with_a_single_share_operation(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(Share(mock_dataset.fields.votes, over=mock_dataset.fields.state))
        mock_widget.transform = Mock()

        dimensions = mock_dataset.fields.timestamp, mock_dataset.fields.state, mock_dataset.fields.political_party

        mock_dataset.query \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, FieldMatcher(mock_dataset.fields.state), ANY)

    def test_find_share_dimensions_with_a_multiple_share_operations(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(Share(mock_dataset.fields.votes, over=mock_dataset.fields.state),
                               Share(mock_dataset.fields.wins, over=mock_dataset.fields.state))
        mock_widget.transform = Mock()

        dimensions = mock_dataset.fields.timestamp, mock_dataset.fields.state, mock_dataset.fields.political_party

        mock_dataset.query \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, FieldMatcher(mock_dataset.fields.state), ANY)

    def test_find_share_dimensions_with_a_multiple_share_operations_over_different_dimensions(self,
                                                                                              mock_fetch_data: Mock,
                                                                                              mock_paginate: Mock):
        mock_widget = f.Widget(Share(mock_dataset.fields.votes, over=mock_dataset.fields.state),
                               Share(mock_dataset.fields.wins, over=mock_dataset.fields.political_party))
        mock_widget.transform = Mock()

        dimensions = mock_dataset.fields.timestamp, mock_dataset.fields.state, mock_dataset.fields.political_party

        mock_dataset.query \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        expected = FieldMatcher(mock_dataset.fields.state, mock_dataset.fields.political_party)
        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, expected, ANY)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch('fireant.queries.builder.dataset_query_builder.paginate')
@patch('fireant.queries.builder.dataset_query_builder.fetch_data')
class QueryBuilderFetchDataTests(TestCase):
    def test_pass_slicer_database_as_arg(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        mock_dataset.query \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(mock_dataset.database, ANY, ANY, ANY, ANY)

    def test_pass_query_from_builder_as_arg(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        mock_dataset.query \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY,
                                                [PypikaQueryMatcher('SELECT SUM("votes") "$votes" '
                                                                    'FROM "politics"."politician"')],
                                                ANY,
                                                ANY,
                                                ANY)

    def test_builder_dimensions_as_arg_with_zero_dimensions(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        mock_dataset.query \
            .widget(mock_widget) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, [], ANY, ANY)

    def test_builder_dimensions_as_arg_with_one_dimension(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = [mock_dataset.fields.state]

        mock_dataset.query \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, FieldMatcher(*dimensions), ANY, ANY)

    def test_builder_dimensions_as_arg_with_multiple_dimensions(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = mock_dataset.fields.timestamp, mock_dataset.fields.state, mock_dataset.fields.political_party

        mock_dataset.query \
            .widget(mock_widget) \
            .dimension(*dimensions) \
            .fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, FieldMatcher(*dimensions), ANY, ANY)

    def test_call_transform_on_widget(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query \
            .dimension(mock_dataset.fields.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_widget.transform.assert_called_once_with(mock_paginate.return_value,
                                                      mock_dataset,
                                                      FieldMatcher(mock_dataset.fields.timestamp),
                                                      [])

    def test_returns_results_from_widget_transform(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        result = mock_dataset.query \
            .dimension(mock_dataset.fields.timestamp) \
            .widget(mock_widget) \
            .fetch()

        self.assertListEqual(result, [mock_widget.transform.return_value])


@patch('fireant.queries.builder.dataset_query_builder.scrub_totals_from_share_results', side_effect=lambda *args: args[0])
@patch('fireant.queries.builder.dataset_query_builder.paginate')
@patch('fireant.queries.builder.dataset_query_builder.fetch_data')
class QueryBuilderPaginationTests(TestCase):
    def test_paginate_is_called(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()
        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query \
            .dimension(mock_dataset.fields.timestamp) \
            .widget(mock_widget) \
            .fetch()

        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=None, offset=None, orders=[])

    def test_pagination_applied_with_limit(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query \
            .dimension(mock_dataset.fields.timestamp) \
            .widget(mock_widget) \
            .limit(15) \
            .fetch()

        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=15, offset=None, orders=[])

    def test_pagination_applied_with_offset(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query \
            .dimension(mock_dataset.fields.timestamp) \
            .widget(mock_widget) \
            .limit(15) \
            .offset(20) \
            .fetch()

        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=15, offset=20, orders=[])

    def test_pagination_applied_with_orders(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query \
            .dimension(mock_dataset.fields.timestamp) \
            .widget(mock_widget) \
            .orderby(mock_dataset.fields.votes, Order.asc) \
            .fetch()

        votes_definition_with_alias_matcher = PypikaQueryMatcher(fn.Sum(mock_dataset.table.votes).as_('$votes'))
        orders = [(votes_definition_with_alias_matcher, Order.asc)]
        mock_paginate.assert_called_once_with(mock_fetch_data.return_value, [mock_widget],
                                              limit=None, offset=None, orders=orders)
