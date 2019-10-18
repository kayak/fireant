from unittest import TestCase
from unittest.mock import Mock

from pypika import Field

from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware
from fireant.database import Database
from fireant.utils import read_csv


class TestBaseDatabase(TestCase):
    def test_database_api(self):
        db = Database()

        with self.assertRaises(NotImplementedError):
            db.connect()

        with self.assertRaises(NotImplementedError):
            db.trunc_date(Field('abc'), 'day')

    def test_to_char(self):
        db = Database()

        to_char = db.to_char(Field('field'))
        self.assertEqual(str(to_char), 'CAST("field" AS VARCHAR)')

    def test_no_concurrency_middleware_specified_gives_default_threadpool(self):
        db = Database(max_processes=5)

        self.assertIsInstance(db.concurrency_middleware, ThreadPoolConcurrencyMiddleware)
        self.assertEquals(db.concurrency_middleware.max_processes, 5)


class TestCSVExport(TestCase):
    def test_export_csv(self):
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1, 'a', True), (2, 'ab', False)]
        mock_cursor.execute = Mock()

        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        ntf = Database.export_csv(mock_connection, 'SELECT a FROM abc')
        ntf_rows = read_csv(ntf.name)

        mock_cursor.execute.assert_called_once_with('SELECT a FROM abc')
        self.assertEqual(['1', 'a', 'True'], ntf_rows[0])
        self.assertEqual(['2', 'ab', 'False'], ntf_rows[1])
