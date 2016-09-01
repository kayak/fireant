# coding: utf-8
import unittest

import pandas as pd

from fireant.slicer.transformers import Transformer


class TransformerTests(unittest.TestCase):
    def test_transformer_api(self):
        tx = Transformer()

        with self.assertRaises(NotImplementedError):
            tx.transform(pd.DataFrame(), {})
