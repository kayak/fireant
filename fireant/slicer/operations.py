# coding: utf8


class Operation(object):
    """
    The `Operation` class represents an operation in the `Slicer` object.
    """

    def __init__(self, key):
        self.key = key


class Totals(Operation):
    def __init__(self, *dimension_keys):
        super(Totals, self).__init__('totals')
        self.dimension_keys = dimension_keys
