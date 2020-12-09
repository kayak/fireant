from unittest import TestCase

from pypika.queries import Column as PypikaColumn

from fireant.database import Column, ColumnsTransformer, MySQLDatabase, TypeEngine, VerticaDatabase, make_columns
from fireant.database.sql_types import (
    ANSIType,
    Char,
    Integer,
    VarChar,
)


class TestColumns(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.type_engine = TypeEngine(
            db_to_ansi_mapper={
                'char': Char,
                'int': Integer,
            },
            ansi_to_db_mapper={
                'CHAR': 'char',
                'INTEGER': 'integer',
            },
        )

    def test_column_representation(self):
        col_a, col_b = Column('a', Char()), Column('b', Char(100))

        with self.subTest('without argument'):
            self.assertEqual('a CHAR', str(col_a))

        with self.subTest('with argument'):
            self.assertEqual('b CHAR(100)', str(col_b))

    def test_as_database_column(self):
        col_a, col_b = Column('a', Char()), Column('b', Char(100))

        with self.subTest('without argument'):
            self.assertEqual('a char', col_a.to_database_column(self.type_engine))

        with self.subTest('with argument'):
            self.assertEqual('b char(100)', col_b.to_database_column(self.type_engine))

    def test_as_pypika_column(self):
        col_a, col_b = Column('a', Char()), Column('b', Char(100))

        pypika_col_a, pypika_col_b = col_a.to_pypika_column(self.type_engine), col_b.to_pypika_column(self.type_engine)

        with self.subTest('without argument'):
            self.assertTrue(isinstance(pypika_col_a, PypikaColumn))
            self.assertEqual('"a" char', str(pypika_col_a))

        with self.subTest('with argument'):
            self.assertTrue(isinstance(pypika_col_b, PypikaColumn))
            self.assertEqual('"b" char(100)', str(pypika_col_b))

    def test_make_columns(self):
        db_cols = [['a', 'char'], ['b', 'int']]

        cols = make_columns(db_cols, self.type_engine)

        self.assertEqual(len(cols), 2)

        for col in cols:
            with self.subTest('test return types'):
                self.assertTrue(isinstance(col, Column))
                self.assertTrue(isinstance(col.type, ANSIType))


class TestColumnsTransformer(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mysql = MySQLDatabase(database='mysql')
        cls.vertica = VerticaDatabase()

        cls.db_cols = [['a', 'nvarchar'], ['b', 'varchar(100)']]
        cls.ansi_cols = [Column('a', VarChar()), Column('b', VarChar(100))]

    def test_database_to_ansi(self):
        ansi_cols = ColumnsTransformer.database_to_ansi(self.db_cols, self.mysql)

        self.assertEqual(ansi_cols, self.ansi_cols)

    def test_ansi_to_database(self):
        db_cols = ColumnsTransformer.ansi_to_database(self.ansi_cols, self.mysql)

        self.assertEqual('a', db_cols[0].name)
        self.assertEqual('b', db_cols[1].name)
        self.assertEqual('varchar', db_cols[0].type)
        self.assertEqual('varchar(100)', db_cols[1].type)

    def test_database_to_database(self):
        db_cols = ColumnsTransformer.database_to_database(self.db_cols, self.mysql, self.vertica)

        self.assertEqual('a', db_cols[0].name)
        self.assertEqual('b', db_cols[1].name)
        self.assertEqual('varchar', db_cols[0].type)
        self.assertEqual('varchar(100)', db_cols[1].type)
