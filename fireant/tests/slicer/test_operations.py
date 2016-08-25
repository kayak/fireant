# coding: utf-8
from unittest import TestCase

from fireant.slicer.operations import Totals


class TotalsTests(TestCase):
    def test_totals_init(self):
        totals = Totals('date')
        self.assertEqual('totals', totals.key)
