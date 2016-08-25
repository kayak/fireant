# coding: utf-8


class Transformer(object):
    def transform(self, dataframe, display_schema):
        raise NotImplementedError


class TransformationException(Exception):
    pass
