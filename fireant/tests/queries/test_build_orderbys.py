from unittest import TestCase

from pypika import Order

import fireant as f
from fireant.tests.dataset.mocks import mock_dataset

timestamp_daily = f.day(mock_dataset.fields.timestamp)


class QueryBuilderOrderTests(TestCase):
    maxDiff = None

    def test_build_query_order_by(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(timestamp_daily)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_asc(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(timestamp_daily, orientation=Order.asc)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" ASC '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_desc(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(timestamp_daily, orientation=Order.desc)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" DESC '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_aggregate_field(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(mock_dataset.fields.votes)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$votes" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_aggregate_field_asc(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(mock_dataset.fields.votes, orientation=Order.asc)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$votes" ASC '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_aggregate_field_desc(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(mock_dataset.fields.votes, orientation=Order.desc)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$votes" DESC '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_multiple_dimensions(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily, mock_dataset.fields['candidate-name'])
            .orderby(timestamp_daily)
            .orderby(mock_dataset.fields['candidate-name'])
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"candidate_name" "$candidate-name",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$candidate-name" '
            'ORDER BY "$timestamp","$candidate-name" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_multiple_dimensions_with_different_orientations(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily, mock_dataset.fields['candidate-name'])
            .orderby(timestamp_daily, orientation=Order.desc)
            .orderby(mock_dataset.fields['candidate-name'], orientation=Order.asc)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            '"candidate_name" "$candidate-name",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp","$candidate-name" '
            'ORDER BY "$timestamp" DESC,"$candidate-name" ASC '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_aggregate_fields_and_fields(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(timestamp_daily)
            .orderby(mock_dataset.fields.votes)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp","$votes" '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_aggregate_fields_and_fields_with_different_orientations(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(timestamp_daily, orientation=Order.asc)
            .orderby(mock_dataset.fields.votes, orientation=Order.desc)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$timestamp" ASC,"$votes" DESC '
            'LIMIT 200000',
            str(queries[0]),
        )

    def test_build_query_order_by_field_not_selected_in_query_added_to_selects(self):
        queries = (
            mock_dataset.query.widget(f.Pandas(mock_dataset.fields.votes))
            .dimension(timestamp_daily)
            .orderby(mock_dataset.fields.wins)
            .sql
        )

        self.assertEqual(len(queries), 1)

        self.assertEqual(
            'SELECT '
            'TRUNC("timestamp",\'DD\') "$timestamp",'
            'SUM("votes") "$votes",'
            'SUM("is_winner") "$wins" '
            'FROM "politics"."politician" '
            'GROUP BY "$timestamp" '
            'ORDER BY "$wins" '
            'LIMIT 200000',
            str(queries[0]),
        )
