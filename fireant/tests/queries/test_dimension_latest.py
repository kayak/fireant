from unittest import TestCase

from fireant.tests.dataset.mocks import mock_dataset


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsChoicesQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_latest_value_for_timestamp_dimension(self):
        query = mock_dataset.latest(mock_dataset.fields.timestamp).sql[0]

        self.assertEqual('SELECT ' 'MAX("timestamp") "$timestamp" ' 'FROM "politics"."politician"', str(query))

    def test_query_latest_value_for_timestamp_dimension_using_join(self):
        query = mock_dataset.latest(mock_dataset.fields.join_timestamp).sql[0]

        self.assertEqual(
            'SELECT '
            'MAX("voter"."timestamp") "$join_timestamp" '
            'FROM "politics"."politician" '
            'JOIN "politics"."voter" ON "politician"."id"="voter"."politician_id"',
            str(query),
        )

    def test_query_latest_value_for_multiple_dimensions(self):
        query = mock_dataset.latest(mock_dataset.fields.timestamp, mock_dataset.fields.timestamp2).sql[0]

        self.assertEqual(
            'SELECT '
            'MAX("timestamp") "$timestamp",'
            'MAX("timestamp2") "$timestamp2" '
            'FROM "politics"."politician"',
            str(query),
        )
