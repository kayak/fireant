from unittest import TestCase
from unittest.mock import MagicMock

from fireant.dataset.klass import _Container


class SlicerContainerTests(TestCase):
    def test_duplicate_items_not_allowed(self):
        item_1 = MagicMock(alias="hi")
        container = _Container([item_1], key_attribute="alias")
        item_2 = MagicMock(alias="hi")

        with self.assertRaises(ValueError) as exception_context:
            container.add(item_2)

        self.assertIn("already exists", str(exception_context.exception))

    def test_reserved_word_not_allowed(self):
        item_1 = MagicMock(alias="hi")
        container = _Container([item_1], key_attribute="alias")
        item_2 = MagicMock(alias="add")

        with self.assertRaises(ValueError) as exception_context:
            container.add(item_2)

        self.assertIn("Reserved name", str(exception_context.exception))
