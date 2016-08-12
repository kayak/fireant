# coding: utf-8
from unittest import TestCase

from fireant.slicer import Slicer
from pypika import Table


class ManagerInitializationTests(TestCase):
    def test_transformers(self):
        slicer = Slicer(Table('test'))

        self.assertTrue(hasattr(slicer, 'manager'))
        self.assertTrue(hasattr(slicer, 'notebook'))
        self.assertTrue(hasattr(slicer, 'highcharts'))
        self.assertTrue(hasattr(slicer, 'datatables'))
