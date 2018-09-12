from unittest import TestCase

from fireant import UniqueDimension
from pypika import Table

test = Table('test')


class UniqueDimensionTests(TestCase):
    def test_display_attribute_exists_on_dimension_with_display_definition(self):
        dim = UniqueDimension('test',
                              definition=test.definition,
                              display_definition=test.display_definition)
        self.assertTrue(hasattr(dim, 'display'))

    def test_display_attribute_does_not_exist_on_dimension_with_no_display_definition(self):
        dim = UniqueDimension('test',
                              definition=test.definition)
        self.assertFalse(hasattr(dim, 'display'))

    def test_display_key_is_set_when_display_definition_is_used(self):
        dim = UniqueDimension('test',
                              definition=test.definition,
                              display_definition=test.display_definition)

        self.assertEqual(dim.display.key, "test_display")
