from unittest import TestCase

from pypika import Table

from fireant import UniqueDimension

test = Table('test')


class UniqueDimensionTests(TestCase):
    def test_display_key_is_set_when_display_definition_is_used(self):
        dim = UniqueDimension('test',
                              definition=test.definition,
                              display_definition=test.display_definition)

        self.assertEqual(dim.display_key, "test_display")

    def test_display_key_is_none_when_display_definition_is_none(self):
        dim = UniqueDimension('test',
                              definition=test.definition)

        self.assertIsNone(dim.display_key)
