from unittest import TestCase

from fireant.database import Database
from pypika import Field

from fireant.middleware.concurrency import ThreadPoolConcurrencyMiddleware


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

    def test_no_concurrency_middleware_specified_gives_default_threadpool(self):
        db = Database(max_processes=5)

        self.assertIsInstance(db.concurrency_middleware, ThreadPoolConcurrencyMiddleware)
        self.assertEquals(db.concurrency_middleware.max_processes, 5)
