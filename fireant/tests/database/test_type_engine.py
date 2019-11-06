from unittest import TestCase
from unittest.mock import patch

from pypika.queries import Column as PypikaColumn

from fireant.database import (
    TypeEngine,
    Column,
    make_columns,
)

from fireant.database.sql_types import (
    ANSIType,
    Char,
    Integer,
)


class TestBaseTypeEngine(TestCase):
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
              }
        )

    def test_split_data_type_without_argument(self):
        db_type = 'varchar'

        raw_type, raw_arguments = TypeEngine.split_data_type(db_type)

        self.assertEqual(raw_type, 'varchar')
        self.assertEqual(len(raw_arguments), 0)

    def test_split_data_type_with_single_argument(self):
        db_type = 'varchar(255)'

        raw_type, raw_arguments = TypeEngine.split_data_type(db_type)

        self.assertEqual(raw_type, 'varchar')
        self.assertEqual(len(raw_arguments), 1)
        self.assertEqual(raw_arguments[0], '255')

    def test_split_data_type_with_multiple_arguments(self):
        db_type = 'numeric(10, 5)'

        raw_type, raw_arguments = TypeEngine.split_data_type(db_type)

        self.assertEqual(raw_type, 'numeric')
        self.assertEqual(len(raw_arguments), 2)
        self.assertEqual(raw_arguments[0], '10')
        self.assertEqual(raw_arguments[1], '5')

    def test_split_data_type_empty_string(self):
        db_type = ''

        raw_type, raw_arguments = TypeEngine.split_data_type(db_type)

        self.assertEqual(raw_type, '')
        self.assertEqual(len(raw_arguments), 0)

    def test_to_ansi_returns_ansi_type_instance_for_known_type(self):
        db_type = 'int'

        ansi_type = self.type_engine.to_ansi(db_type)

        self.assertTrue(isinstance(ansi_type, ANSIType))
        self.assertTrue(isinstance(ansi_type, Integer))

    def test_to_ansi_instantiates_ansi_type_with_arguments(self):
        db_type = 'char(100)'

        ansi_type = self.type_engine.to_ansi(db_type)

        self.assertEqual(ansi_type.length, '100')

    def test_to_ansi_raises_error_for_unknown_type(self):
        db_type = 'smallint'

        with self.assertRaises(ValueError):
            self.type_engine.to_ansi(db_type)

    @patch.object(TypeEngine, 'split_data_type', return_value=('int', []))
    def test_to_ansi_converts_input_to_lower_case(self, mock_split_data):
        db_type = 'INT'

        self.type_engine.to_ansi(db_type)

        mock_split_data.assert_called_once_with('int')

    def test_from_ansi(self):
        ansi_type = Integer()

        db_type = self.type_engine.from_ansi(ansi_type)

        self.assertEqual('integer', db_type)


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
              }
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
            self.assertEqual('a char', col_a.as_database_column(self.type_engine))

        with self.subTest('with argument'):
            self.assertEqual('b char(100)', col_b.as_database_column(self.type_engine))

    def test_as_pypika_column(self):
        col_a, col_b = Column('a', Char()), Column('b', Char(100))

        pypika_col_a, pypika_col_b = col_a.as_pypika_column(self.type_engine), col_b.as_pypika_column(self.type_engine)

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
