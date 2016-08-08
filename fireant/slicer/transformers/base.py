# coding: utf-8


class Transformer(object):
    def transform(self, data_frame, display_schema):
        raise NotImplementedError()


class TransformationException(Exception):
    pass
