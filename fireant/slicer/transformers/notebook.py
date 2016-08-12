# coding: utf-8
from . import Transformer


class MatplotlibTransformer(Transformer):
    def transform(self, data_frame, display_schema):
        return data_frame.plot()


class PandasTransformer(Transformer):
    def transform(self, data_frame, display_schema):
        return data_frame.plot()
