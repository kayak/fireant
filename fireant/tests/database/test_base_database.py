from unittest import TestCase

from fireant.database import Database
from pypika import Field


class DatabaseTests(TestCase):
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
