from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

from pypika import Field

from fireant.database import Database
from fireant.middleware.decorators import connection_middleware


@connection_middleware
def test_fetch(database, query, **kwargs):
    return kwargs.get('connection')


def test_connect():
    mock_connection = Mock()
    mock_connection.__enter__ = Mock()
    mock_connection.__exit__ = Mock()
    return mock_connection


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

    def test_no_custom_middlewares_specified_still_gives_connection_middleware(self):
        db = Database()

        self.assertEqual(1, len(db.middlewares))
        self.assertIs(db.middlewares[0], connection_middleware)

    @patch.object(Database, 'fetch')
    @patch.object(Database, 'connect')
    def test_database_reuse_passed_connection(self, mock_connect, mock_fetch):
        db = Database()

        mock_connect.side_effect = test_connect
        mock_fetch.side_effect = test_fetch

        with db.connect() as connection:
            connection_1 = db.fetch(db, 'SELECT a from abc', connection=connection)
            connection_2 = db.fetch(db, 'SELECT b from def', connection=connection)

        self.assertEqual(1, mock_connect.call_count)
        self.assertEqual(connection_1, connection_2)

    @patch.object(Database, 'fetch')
    @patch.object(Database, 'connect')
    def test_database_opens_new_connection(self, mock_connect, mock_fetch):
        db = Database()

        mock_connect.side_effect = test_connect
        mock_fetch.side_effect = test_fetch

        connection_1 = db.fetch(db, 'SELECT a from abc')
        connection_2 = db.fetch(db, 'SELECT b from def')

        self.assertEqual(2, mock_connect.call_count)
        self.assertNotEqual(connection_1, connection_2)
