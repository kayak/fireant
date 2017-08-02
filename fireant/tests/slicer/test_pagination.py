from pypika import Order

from fireant.slicer import Paginator
from unittest import TestCase


class PaginatorObjectTests(TestCase):
    def test_offset_and_limit_have_default_0(self):
        paginator = Paginator()
        self.assertEqual(paginator.limit, 0)
        self.assertEqual(paginator.offset, 0)

    def test_orderby_empty_tuple_by_default(self):
        paginator = Paginator()
        self.assertEqual(paginator.order, ())

    def test_object__str__formatting(self):
        paginator = Paginator(offset=10, limit=20, order=[('clicks', Order.desc)])
        self.assertEqual(str(paginator), 'offset: 10 limit: 20 order: [(\'clicks\', \'desc\')]')
