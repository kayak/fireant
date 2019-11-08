from unittest import TestCase
from unittest.mock import patch

from fireant.database import (
    MySQLDatabase,
    MySQLTypeEngine,
    VerticaDatabase,
    VerticaTypeEngine,
    DataBlender,
    Column,
)
from fireant.database.sql_types import VarChar


class TestDataBlending(TestCase):
    @classmethod
    def setUpClass(cls):
        mysql = MySQLDatabase(database='mysql')
        mysql_type_engine = MySQLTypeEngine()

        vertica = VerticaDatabase()
        vertica_type_engine = VerticaTypeEngine()

        cls.data_blender_m = DataBlender(
              primary_db=mysql,
              primary_type_engine=mysql_type_engine,
              secondary_db=vertica,
              secondary_type_engine=vertica_type_engine
        )
        cls.data_blender_v = DataBlender(
              primary_db=vertica,
              primary_type_engine=vertica_type_engine,
              secondary_db=mysql,
              secondary_type_engine=mysql_type_engine
        )

        cls.cols = Column('a', VarChar()), Column('b', VarChar(100))

    @patch.object(VerticaDatabase, 'get_column_definitions', return_value=[['a', 'varchar2'], ['b', 'varchar(100)']])
    @patch.object(MySQLDatabase, 'get_column_definitions', return_value=[['a', 'nvarchar'], ['b', 'varchar(100)']])
    def test_derive_columns_from_secondary(self, mock_mysql_get_columns, mock_vertica_get_columns):
        for data_blender in [self.data_blender_m, self.data_blender_v]:
            with self.subTest(data_blender.secondary_db.database):
                cols = data_blender.derive_columns_from_secondary('test_schema', 'test_table')

                self.assertEqual(self.cols[0], cols[0])
                self.assertEqual(self.cols[1], cols[1])

    def test_transform_columns_to_primary(self):
        db_col_1, db_col_2 = self.data_blender_m.transform_columns_to_primary(self.cols)

        self.assertEqual('a', db_col_1.name)
        self.assertEqual('b', db_col_2.name)
        self.assertEqual('varchar', db_col_1.type)
        self.assertEqual('varchar(100)', db_col_2.type)

