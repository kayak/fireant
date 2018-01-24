from unittest import TestCase

from fireant.slicer.queries.database import fetch_data
from unittest.mock import Mock


class FetchDataTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_database = Mock(name='database')
        cls.mock_data_frame = cls.mock_database.fetch_data.return_value = Mock(name='data_frame')
        cls.mock_query = 'SELECT *'
        cls.mock_index_levels = ['a', 'b']

        cls.result = fetch_data(cls.mock_database, cls.mock_query, cls.mock_index_levels)

    def test_fetch_data_called_on_database(self):
        self.mock_database.fetch_data.assert_called_once_with(self.mock_query)

    def test_index_set_on_data_frame_result(self):
        self.mock_data_frame.set_index.assert_called_once_with(self.mock_index_levels)
