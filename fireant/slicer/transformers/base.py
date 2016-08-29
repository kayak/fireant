# coding: utf-8


class Transformer(object):
    def prevalidate_request(self, slicer, metrics, dimensions,
                            metric_filters, dimension_filters,
                            references, operations):
        pass

    def transform(self, dataframe, display_schema):
        raise NotImplementedError


class TransformationException(Exception):
    pass
