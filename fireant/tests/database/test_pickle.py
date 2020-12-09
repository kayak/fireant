import pickle
from unittest.case import TestCase

from fireant import (
    MySQLDatabase,
    PostgreSQLDatabase,
    RedshiftDatabase,
    SnowflakeDatabase,
    VerticaDatabase,
)


class DatabasePickleTests(TestCase):
    def test_that_database_inst_can_be_pickled(self):
        for db_inst in (
            MySQLDatabase(),
            PostgreSQLDatabase(),
            RedshiftDatabase(),
            SnowflakeDatabase(),
            VerticaDatabase(),
        ):
            with self.subTest('serialization {}'.format(db_inst.__class__.__name__)):
                dump = pickle.dumps(db_inst, pickle.HIGHEST_PROTOCOL)
                self.assertIsNotNone(dump)

            with self.subTest('deserialization {}'.format(db_inst.__class__.__name__)):
                db_pickle = pickle.loads(dump)
                self.assertIsNotNone(db_pickle, dump)
