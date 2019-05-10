from unittest import TestCase

from fireant import DataType


class DataTypeTests(TestCase):
    def test_repr_of_date(self):
        self.assertEquals('date', repr(DataType.date))

    def test_repr_of_number(self):
        self.assertEquals('number', repr(DataType.number))

    def test_repr_of_boolean(self):
        self.assertEquals('boolean', repr(DataType.boolean))

    def test_repr_of_text(self):
        self.assertEquals('text', repr(DataType.text))
