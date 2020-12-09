from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from fireant import DataSet, DataType, Field
from fireant.tests.dataset.mocks import mock_dataset, politicians_table, test_database


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class DimensionsLatestQueryBuilderTests(TestCase):
    maxDiff = None

    def test_query_single_dimension(self):
        query = mock_dataset.latest(mock_dataset.fields.timestamp).sql[0]

        self.assertEqual('SELECT ' 'MAX("timestamp") "$timestamp" ' 'FROM "politics"."politician"', str(query))

    def test_query_single_dimension_with_join(self):
        query = mock_dataset.latest(mock_dataset.fields.join_timestamp).sql[0]

        self.assertEqual(
            'SELECT '
            'MAX("voter"."timestamp") "$join_timestamp" '
            'FROM "politics"."politician" '
            'JOIN "politics"."voter" ON "politician"."id"="voter"."politician_id"',
            str(query),
        )

    def test_query_multiple_dimensions(self):
        query = mock_dataset.latest(mock_dataset.fields.timestamp, mock_dataset.fields.timestamp2).sql[0]

        self.assertEqual(
            'SELECT '
            'MAX("timestamp") "$timestamp",'
            'MAX("timestamp2") "$timestamp2" '
            'FROM "politics"."politician"',
            str(query),
        )

    @patch('fireant.queries.builder.dimension_latest_query_builder.fetch_data')
    def test_envelopes_responses_if_return_additional_metadata_True(self, mock_fetch_data):
        dataset = DataSet(
            table=politicians_table,
            database=test_database,
            return_additional_metadata=True,
            fields=[
                Field(
                    "timestamp1",
                    label="timestamp1",
                    definition=politicians_table.timestamp1,
                    data_type=DataType.text,
                    hyperlink_template="http://example.com/{political_party}",
                )
            ],
        )

        df = pd.DataFrame({'political_party': ['a', 'b', 'c']}).set_index('political_party')
        mock_fetch_data.return_value = 100, df

        result = dataset.latest(dataset.fields.timestamp1).fetch()

        self.assertEqual(dict(max_rows_returned=100), result['metadata'])
        self.assertTrue(result['data'].equals(pd.Series(['a'], index=['political_party'])))
