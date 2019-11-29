from unittest import TestCase

from fireant import DataType


class DataTypeTests(TestCase):
    def test_repr_of_date(self):
        self.assertEqual('date', repr(DataType.date))

    def test_repr_of_number(self):
        self.assertEqual('number', repr(DataType.number))

    def test_repr_of_boolean(self):
        self.assertEqual('boolean', repr(DataType.boolean))

    def test_repr_of_text(self):
        self.assertEqual('text', repr(DataType.text))
