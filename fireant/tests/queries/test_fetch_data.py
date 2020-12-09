import copy
from unittest import TestCase
from unittest.mock import ANY, MagicMock, Mock, patch

import pandas as pd
from pypika import Order, Table

import fireant as f
from fireant import DataSet, DataType, Field, Share
from fireant.dataset.filters import ComparisonOperator
from fireant.dataset.references import ReferenceFilter
from fireant.queries.sets import _make_set_dimension
from fireant.tests.database.mock_database import TestDatabase
from fireant.tests.dataset.matchers import FieldMatcher, PypikaQueryMatcher
from fireant.tests.dataset.mocks import mock_category_annotation_dataset, mock_dataset, mock_date_annotation_dataset


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch("fireant.queries.builder.dataset_query_builder.paginate")
@patch("fireant.queries.builder.dataset_query_builder.fetch_data", return_value=(100, MagicMock()))
class FindShareDimensionsTests(TestCase):
    def test_find_no_share_dimensions_with_no_share_operation(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.state,
            mock_dataset.fields.political_party,
        )

        mock_dataset.query.widget(mock_widget).dimension(*dimensions).fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, [], ANY)

    def test_find_share_dimensions_with_a_single_share_operation(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(Share(mock_dataset.fields.votes, over=mock_dataset.fields.state))
        mock_widget.transform = Mock()

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.state,
            mock_dataset.fields.political_party,
        )

        mock_dataset.query.widget(mock_widget).dimension(*dimensions).fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, FieldMatcher(mock_dataset.fields.state), ANY)

    def test_find_share_dimensions_with_a_multiple_share_operations(self, mock_fetch_data: Mock, mock_paginate: Mock):
        mock_widget = f.Widget(
            Share(mock_dataset.fields.votes, over=mock_dataset.fields.state),
            Share(mock_dataset.fields.wins, over=mock_dataset.fields.state),
        )
        mock_widget.transform = Mock()

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.state,
            mock_dataset.fields.political_party,
        )

        mock_dataset.query.widget(mock_widget).dimension(*dimensions).fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, FieldMatcher(mock_dataset.fields.state), ANY)

    def test_find_share_dimensions_with_a_multiple_share_operations_over_different_dimensions(
        self, mock_fetch_data: Mock, mock_paginate: Mock
    ):
        mock_widget = f.Widget(
            Share(mock_dataset.fields.votes, over=mock_dataset.fields.state),
            Share(mock_dataset.fields.wins, over=mock_dataset.fields.political_party),
        )
        mock_widget.transform = Mock()

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.state,
            mock_dataset.fields.political_party,
        )

        mock_dataset.query.widget(mock_widget).dimension(*dimensions).fetch()

        expected = FieldMatcher(mock_dataset.fields.state, mock_dataset.fields.political_party)
        mock_fetch_data.assert_called_once_with(ANY, ANY, ANY, expected, ANY)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@patch("fireant.queries.builder.dataset_query_builder.apply_reference_filters")
@patch("fireant.queries.builder.dataset_query_builder.paginate")
@patch("fireant.queries.builder.dataset_query_builder.fetch_data", return_value=(100, MagicMock()))
class QueryBuilderFetchDataTests(TestCase):
    def test_reference_filters_are_applied(self, mock_fetch: Mock, mock_2: Mock, mock_apply_reference_filters: Mock):
        db = TestDatabase()
        t0 = Table("test0")
        dataset = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
            ],
        )
        mock_widget = f.Widget(dataset.fields.metric0)
        mock_widget.transform = Mock()
        reference_filter = ReferenceFilter(dataset.fields.metric0, ComparisonOperator.gt, 5)
        reference = f.DayOverDay(dataset.fields.timestamp, filters=[reference_filter])

        df = pd.DataFrame.from_dict({"$value": [1]})
        mock_fetch.return_value = 100, df
        mock_apply_reference_filters.return_value = df

        (dataset.query().dimension(dataset.fields.timestamp).widget(mock_widget).reference(reference)).fetch()

        mock_apply_reference_filters.assert_called_once_with(df, reference)

    def test_pass_slicer_database_as_arg(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        mock_dataset.query.widget(mock_widget).fetch()

        mock_fetch_data.assert_called_once_with(mock_dataset.database, ANY, ANY, ANY, ANY)

    def test_pass_query_from_builder_as_arg(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        mock_dataset.query.widget(mock_widget).fetch()

        mock_fetch_data.assert_called_once_with(
            ANY,
            [PypikaQueryMatcher('SELECT SUM("votes") "$votes" FROM "politics"."politician" ORDER BY 1 LIMIT 200000')],
            ANY,
            ANY,
            ANY,
        )

    def test_builder_dimensions_as_arg_with_zero_dimensions(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        mock_dataset.query.widget(mock_widget).fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, [], ANY, ANY)

    def test_builder_dimensions_as_arg_with_one_dimension(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = [mock_dataset.fields.state]

        mock_dataset.query.widget(mock_widget).dimension(*dimensions).fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, FieldMatcher(*dimensions), ANY, ANY)

    def test_builder_dimensions_as_arg_with_one_replaced_set_dimension(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = [mock_dataset.fields.state]

        set_filter = f.ResultSet(dimensions[0] == 'On')
        mock_dataset.query.widget(mock_widget).dimension(*dimensions).filter(set_filter).fetch()

        set_dimension = _make_set_dimension(set_filter, mock_dataset)
        mock_fetch_data.assert_called_once_with(ANY, ANY, FieldMatcher(set_dimension), ANY, ANY)

    def test_builder_dimensions_as_arg_with_a_non_replaced_set_dimension(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = [mock_dataset.fields.state]

        set_filter = f.ResultSet(dimensions[0] == 'On', will_replace_referenced_dimension=False)
        mock_dataset.query.widget(mock_widget).dimension(*dimensions).filter(set_filter).fetch()

        set_dimension = _make_set_dimension(set_filter, mock_dataset)
        mock_fetch_data.assert_called_once_with(ANY, ANY, FieldMatcher(set_dimension, dimensions[0]), ANY, ANY)

    def test_builder_dimensions_as_arg_with_multiple_dimensions(self, mock_fetch_data: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        dimensions = (
            mock_dataset.fields.timestamp,
            mock_dataset.fields.state,
            mock_dataset.fields.political_party,
        )

        mock_dataset.query.widget(mock_widget).dimension(*dimensions).fetch()

        mock_fetch_data.assert_called_once_with(ANY, ANY, FieldMatcher(*dimensions), ANY, ANY)

    def test_call_transform_on_widget(self, mock_fetch_data: Mock, mock_paginate: Mock, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query.dimension(mock_dataset.fields.timestamp).widget(mock_widget).fetch()

        mock_widget.transform.assert_called_once_with(
            mock_paginate.return_value,
            FieldMatcher(mock_dataset.fields.timestamp),
            [],
            None,
        )

    def test_returns_results_from_widget_transform(self, *args):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        result = mock_dataset.query.dimension(mock_dataset.fields.timestamp).widget(mock_widget).fetch()

        self.assertListEqual(result, [mock_widget.transform.return_value])

    def test_envelopes_responses_if_return_additional_metadata_True(self, *args):
        dataset = copy.deepcopy(mock_dataset)
        mock_widget = f.Widget(dataset.fields.votes)
        mock_widget.transform = Mock()
        dataset.return_additional_metadata = True

        result = dataset.query.dimension(dataset.fields.timestamp).widget(mock_widget).fetch()

        self.assertEqual(
            dict(data=[mock_widget.transform.return_value], metadata=dict(max_rows_returned=100)),
            result,
        )


@patch(
    "fireant.queries.builder.dataset_query_builder.scrub_totals_from_share_results",
    side_effect=lambda *args: args[0],
)
@patch("fireant.queries.builder.dataset_query_builder.paginate")
@patch("fireant.queries.builder.dataset_query_builder.fetch_data", return_value=(100, MagicMock()))
class QueryBuilderPaginationTests(TestCase):
    def test_paginate_is_called(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()
        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query.dimension(mock_dataset.fields.timestamp).widget(mock_widget).fetch()

        mock_paginate.assert_called_once_with(
            mock_fetch_data.return_value[1],
            [mock_widget],
            limit=None,
            offset=None,
            orders=[(mock_dataset.fields.timestamp, None)],
        )

    def test_pagination_applied_with_limit(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query.dimension(mock_dataset.fields.timestamp).widget(mock_widget).limit_client(15).fetch()

        mock_paginate.assert_called_once_with(
            mock_fetch_data.return_value[1],
            [mock_widget],
            limit=15,
            offset=None,
            orders=[(mock_dataset.fields.timestamp, None)],
        )

    def test_pagination_applied_with_offset(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query.dimension(mock_dataset.fields.timestamp).widget(mock_widget).limit_client(15).offset_client(
            20
        ).fetch()

        mock_paginate.assert_called_once_with(
            mock_fetch_data.return_value[1],
            [mock_widget],
            limit=15,
            offset=20,
            orders=[(mock_dataset.fields.timestamp, None)],
        )

    def test_pagination_applied_with_orders(self, mock_fetch_data: Mock, mock_paginate: Mock, *mocks):
        mock_widget = f.Widget(mock_dataset.fields.votes)
        mock_widget.transform = Mock()

        # Need to keep widget the last call in the chain otherwise the object gets cloned and the assertion won't work
        mock_dataset.query.dimension(mock_dataset.fields.timestamp).widget(mock_widget).orderby(
            mock_dataset.fields.votes, Order.asc
        ).fetch()

        orders = [(mock_dataset.fields.votes, Order.asc)]
        mock_paginate.assert_called_once_with(
            mock_fetch_data.return_value[1],
            [mock_widget],
            limit=None,
            offset=None,
            orders=orders,
        )


@patch("fireant.queries.builder.dataset_query_builder.fetch_data", return_value=(100, MagicMock()))
class QueryBuilderAnnotationTests(TestCase):
    def get_fetch_call_args(self, mock_fetch_data):
        self.assertEqual(mock_fetch_data.call_count, 2)

        fetch_annotation_data, fetch_data = mock_fetch_data.mock_calls[:2]
        _, fetch_annotation_args, _ = fetch_annotation_data
        _, fetch_data_args, _ = fetch_data

        return fetch_annotation_args, fetch_data_args

    @classmethod
    def setUpClass(cls):
        cls.mock_widget = f.Widget(mock_date_annotation_dataset.fields.votes)
        cls.mock_widget.transform = Mock()

    def test_fetch_annotation_single_date_dimension(self, mock_fetch_data: Mock):
        dims = [mock_date_annotation_dataset.fields.timestamp]

        mock_date_annotation_dataset.query.widget(self.mock_widget).dimension(*dims).fetch()
        fetch_annotation_args, fetch_data_args = self.get_fetch_call_args(mock_fetch_data)

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp2" "$timestamp2",'
                        '"district_name" "$district-name" '
                        'FROM "politics"."annotations" '
                        'GROUP BY "$timestamp2","$district-name"'
                    )
                ],
                FieldMatcher(mock_date_annotation_dataset.annotation.alignment_field),
            ),
            fetch_annotation_args,
        )

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp" "$timestamp",'
                        'SUM("votes") "$votes" '
                        'FROM "politics"."politician" '
                        'GROUP BY "$timestamp" '
                        'ORDER BY "$timestamp" '
                        'LIMIT 200000'
                    )
                ],
                FieldMatcher(*dims),
                [],
                [],
            ),
            fetch_data_args,
        )

    def test_fetch_annotation_single_category_dimension(self, mock_fetch_data: Mock):
        dims = [mock_category_annotation_dataset.fields.political_party]
        widget = f.Widget(mock_category_annotation_dataset.fields.votes)
        widget.transform = Mock()

        mock_category_annotation_dataset.query.widget(widget).dimension(*dims).fetch()
        fetch_annotation_args, fetch_data_args = self.get_fetch_call_args(mock_fetch_data)

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"political_party" "$political_party",'
                        '"district_name" "$district-name" '
                        'FROM "politics"."annotations" '
                        'GROUP BY "$political_party","$district-name"'
                    )
                ],
                FieldMatcher(mock_category_annotation_dataset.annotation.alignment_field),
            ),
            fetch_annotation_args,
        )

        self.assertEqual(
            (
                mock_category_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"political_party" "$political_party",'
                        'SUM("votes") "$votes" '
                        'FROM "politics"."politician" '
                        'GROUP BY "$political_party" '
                        'ORDER BY "$political_party" '
                        'LIMIT 200000'
                    )
                ],
                FieldMatcher(*dims),
                [],
                [],
            ),
            fetch_data_args,
        )

    def test_fetch_annotation_multiple_dimensions(self, mock_fetch_data: Mock):
        dims = [
            mock_date_annotation_dataset.fields.timestamp,
            mock_date_annotation_dataset.fields.political_party,
        ]

        mock_date_annotation_dataset.query.widget(self.mock_widget).dimension(*dims).fetch()
        fetch_annotation_args, fetch_data_args = self.get_fetch_call_args(mock_fetch_data)

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp2" "$timestamp2",'
                        '"district_name" "$district-name" '
                        'FROM "politics"."annotations" '
                        'GROUP BY "$timestamp2","$district-name"'
                    )
                ],
                FieldMatcher(mock_date_annotation_dataset.annotation.alignment_field),
            ),
            fetch_annotation_args,
        )

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp" "$timestamp",'
                        '"political_party" "$political_party",'
                        'SUM("votes") "$votes" '
                        'FROM "politics"."politician" '
                        'GROUP BY "$timestamp","$political_party" '
                        'ORDER BY "$timestamp","$political_party" '
                        'LIMIT 200000'
                    )
                ],
                FieldMatcher(*dims),
                [],
                [],
            ),
            fetch_data_args,
        )

    def test_fetch_annotation_invalid_first_dimension(self, mock_fetch_data: Mock):
        dims = [mock_date_annotation_dataset.fields.political_party]

        mock_date_annotation_dataset.query.widget(self.mock_widget).dimension(*dims).fetch()

        mock_fetch_data.assert_called_once_with(
            mock_date_annotation_dataset.database,
            [
                PypikaQueryMatcher(
                    "SELECT "
                    '"political_party" "$political_party",'
                    'SUM("votes") "$votes" '
                    'FROM "politics"."politician" '
                    'GROUP BY "$political_party" '
                    'ORDER BY "$political_party" '
                    'LIMIT 200000'
                )
            ],
            FieldMatcher(*dims),
            [],
            [],
        )

    def test_fetch_annotation_no_dimension(self, mock_fetch_data: Mock):
        dims = []

        mock_date_annotation_dataset.query.widget(self.mock_widget).dimension(*dims).fetch()

        mock_fetch_data.assert_called_once_with(
            mock_date_annotation_dataset.database,
            [PypikaQueryMatcher('SELECT SUM("votes") "$votes" FROM "politics"."politician" ORDER BY 1 LIMIT 200000')],
            FieldMatcher(*dims),
            [],
            [],
        )

    def test_fetch_annotation_with_filter(self, mock_fetch_data: Mock):
        dims = [mock_date_annotation_dataset.fields.timestamp]

        mock_date_annotation_dataset.query.widget(self.mock_widget).dimension(*dims).filter(
            mock_date_annotation_dataset.fields.timestamp == "2020-01-01"
        ).fetch()
        fetch_annotation_args, fetch_data_args = self.get_fetch_call_args(mock_fetch_data)

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp2" "$timestamp2",'
                        '"district_name" "$district-name" '
                        'FROM "politics"."annotations" '
                        "WHERE \"timestamp2\"='2020-01-01' "
                        'GROUP BY "$timestamp2","$district-name"'
                    )
                ],
                FieldMatcher(mock_date_annotation_dataset.annotation.alignment_field),
            ),
            fetch_annotation_args,
        )

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp" "$timestamp",'
                        'SUM("votes") "$votes" '
                        'FROM "politics"."politician" '
                        "WHERE \"timestamp\"='2020-01-01' "
                        'GROUP BY "$timestamp" '
                        'ORDER BY "$timestamp" '
                        'LIMIT 200000'
                    )
                ],
                FieldMatcher(*dims),
                [],
                [],
            ),
            fetch_data_args,
        )

    def test_fetch_annotation_invalid_filter(self, mock_fetch_data: Mock):
        dims = [
            mock_date_annotation_dataset.fields.timestamp,
            mock_date_annotation_dataset.fields.political_party,
        ]

        mock_date_annotation_dataset.query.widget(self.mock_widget).dimension(*dims).filter(
            mock_date_annotation_dataset.fields.political_party == "Democrat"
        ).fetch()
        fetch_annotation_args, fetch_data_args = self.get_fetch_call_args(mock_fetch_data)

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp2" "$timestamp2",'
                        '"district_name" "$district-name" '
                        'FROM "politics"."annotations" '
                        'GROUP BY "$timestamp2","$district-name"'
                    )
                ],
                FieldMatcher(mock_date_annotation_dataset.annotation.alignment_field),
            ),
            fetch_annotation_args,
        )

        self.assertEqual(
            (
                mock_date_annotation_dataset.database,
                [
                    PypikaQueryMatcher(
                        "SELECT "
                        '"timestamp" "$timestamp",'
                        '"political_party" "$political_party",'
                        'SUM("votes") "$votes" '
                        'FROM "politics"."politician" '
                        "WHERE \"political_party\"='Democrat' "
                        'GROUP BY "$timestamp","$political_party" '
                        'ORDER BY "$timestamp","$political_party" '
                        'LIMIT 200000'
                    )
                ],
                FieldMatcher(*dims),
                [],
                [],
            ),
            fetch_data_args,
        )
