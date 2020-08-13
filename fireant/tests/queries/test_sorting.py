from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from pypika import Order

from fireant.queries.sorting import sort
from fireant.tests.dataset.mocks import (
    dimx2_date_str_df,
    dimx2_str_num_df,
    dimx3_date_str_str_df,
)

TS = "$timestamp"

mock_table_widget = Mock()
mock_table_widget.group_sort = False

mock_chart_widget = Mock()
mock_chart_widget.group_sort = True

mock_dimension_definition = Mock()
mock_dimension_definition.alias = "$political_party"

mock_metric_definition = Mock()
mock_metric_definition.alias = "$votes"


class SimpleSortingTests(TestCase):
    @patch("fireant.queries.sorting._simple_sort")
    def test_that_with_no_widgets_using_group_sort_that_simple_sort_is_applied(
        self, mock_sort
    ):
        sort(dimx2_date_str_df, [mock_table_widget])

        mock_sort.assert_called_once_with(ANY, ANY)

    @patch("fireant.queries.sorting._simple_sort")
    def test_that_with_group_sort_and_one_dimension_that_simple_sort_is_applied(
        self, mock_sort
    ):
        sort(dimx2_str_num_df, [mock_table_widget])

        mock_sort.assert_called_once_with(ANY, ANY)

    def test_apply_sort_with_one_order_dimension_asc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_table_widget],
            orders=[(mock_dimension_definition, Order.asc)],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[mock_dimension_definition.alias], ascending=True
        )
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_one_order_dimension_desc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_table_widget],
            orders=[(mock_dimension_definition, Order.desc)],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[mock_dimension_definition.alias], ascending=False
        )
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_one_order_metric_asc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_table_widget],
            orders=[(mock_metric_definition, Order.asc)],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[mock_metric_definition.alias], ascending=True
        )
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_one_order_metric_desc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_table_widget],
            orders=[(mock_metric_definition, Order.desc)],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[mock_metric_definition.alias], ascending=False
        )
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_multiple_orders(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_table_widget],
            orders=[
                (mock_dimension_definition, Order.asc),
                (mock_metric_definition, Order.desc),
            ],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[mock_dimension_definition.alias, mock_metric_definition.alias],
            ascending=[True, False],
        )
        assert_frame_equal(expected, _sorted)


class GroupSortTests(TestCase):
    @patch("fireant.queries.sorting._group_sort")
    def test_with_one_widget_using_group_sort_that_group_sort_is_applied(
        self, mock_sort
    ):
        sort(dimx2_date_str_df, [mock_chart_widget, mock_table_widget])

        mock_sort.assert_called_once_with(ANY, ANY)

    def test_apply_sort_with_one_order_dimension_asc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_chart_widget],
            orders=[(mock_dimension_definition, Order.asc)],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[TS, mock_dimension_definition.alias], ascending=True
        )
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_one_order_dimension_desc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_chart_widget],
            orders=[(mock_dimension_definition, Order.desc)],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[TS, mock_dimension_definition.alias], ascending=(True, False)
        )
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_one_order_metric_asc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_chart_widget],
            orders=[(mock_metric_definition, Order.asc)],
        )

        expected = dimx2_date_str_df.iloc[[1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_one_order_metric_desc(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_chart_widget],
            orders=[(mock_metric_definition, Order.desc)],
        )

        expected = dimx2_date_str_df.iloc[[2, 0, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11]]
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_multiple_levels_df(self):
        _sorted = sort(
            dimx3_date_str_str_df,
            [mock_chart_widget],
            orders=[(mock_metric_definition, Order.asc)],
        )

        sorted_groups = (
            dimx3_date_str_str_df.groupby(level=[1, 2])
            .sum()
            .sort_values(by="$votes", ascending=True)
            .index
        )
        expected = (
            dimx3_date_str_str_df.groupby(level=0)
            .apply(lambda df: df.reset_index(level=0, drop=True).reindex(sorted_groups))
            .dropna()
        )
        metrics = ["$votes", "$wins", "$wins_with_style", "$turnout"]
        expected[metrics] = expected[metrics].astype(np.int64)
        assert_frame_equal(expected, _sorted)

    def test_apply_sort_with_multiple_orders(self):
        _sorted = sort(
            dimx2_date_str_df,
            [mock_chart_widget],
            orders=[
                (mock_dimension_definition, Order.asc),
                (mock_metric_definition, Order.desc),
            ],
        )

        expected = dimx2_date_str_df.sort_values(
            by=[TS, mock_dimension_definition.alias, mock_metric_definition.alias],
            ascending=[True, True, False],
        )
        assert_frame_equal(expected, _sorted)

    def test_group_sort_with_order_on_non_selected_datetime_metric(self):
        index_values = [['2016-10-03', '2016-10-04', '2016-10-05'], ['General', 'City']]
        # City values have the highest aggregated $updated_time timestamp (as paginate uses MAX for datetimes):
        data_values = [
            (10, '2018-08-10'),  # General
            (2, '2018-08-11'),  # City
            (44, '2018-08-09'),  # General
            (6, '2018-08-06'),  # City
            (18, '2018-08-04'),  # General
            (21, '2018-08-20')  # City
        ]
        idx = pd.MultiIndex.from_product(index_values, names=['$created_time', '$category'])
        df = pd.DataFrame(data_values, idx, ['$seeds', '$updated_time'])
        df["$updated_time"] = pd.to_datetime(df["$updated_time"])

        _sorted = sort(df, [mock_chart_widget], [(Mock(alias="updated_time"), Order.desc)])

        # We order descending on $updated_time, which puts City values first.
        # So we sort the original dataframe on #category ascending so that it would put the City values first
        expected = df.sort_values(
            by=['$created_time', '$category'],
            ascending=[True, True],
        )
        # This created expected dataframe should match the result
        assert_frame_equal(expected, _sorted)
