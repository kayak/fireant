# coding: utf8


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` object.
    """

    def __init__(self, key):
        self.key = key

    def schemas(self, slicer):
        raise NotImplementedError


class Totals(Operation):
    def __init__(self, *dimension_keys):
        super(Totals, self).__init__('rollup')
        self.dimension_keys = dimension_keys

    def schemas(self, slicer):
        dimensions = []
        for dimension in self.dimension_keys:
            if isinstance(dimension, (list, tuple)):
                dimension, args = dimension[0], dimension[1:]
            else:
                args = []

            schema_dimension = slicer.dimensions.get(dimension)
            for key, definition in schema_dimension.schemas(*args):
                dimensions.append(key)

        return dimensions
