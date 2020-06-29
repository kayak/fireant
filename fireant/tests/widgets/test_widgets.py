from unittest import TestCase

from fireant.widgets.base import (
    TransformableWidget,
    Widget,
)


class BaseWidgetTests(TestCase):
    def test_create_widget_with_items(self):
        widget = Widget(0, 1, 2)
        self.assertListEqual(widget.items, [0, 1, 2])

    def test_add_widget_to_items(self):
        widget = Widget(0, 1, 2).item(3)
        self.assertListEqual(widget.items, [0, 1, 2, 3])

    def test_item_func_immutable(self):
        widget1 = Widget(0, 1, 2)
        widget2 = widget1.item(3)
        self.assertIsNot(widget1, widget2)

    def test_transformable_widget_has_transform_function(self):
        self.assertTrue(hasattr(TransformableWidget, 'transform'))
        self.assertTrue(callable(TransformableWidget.transform))
