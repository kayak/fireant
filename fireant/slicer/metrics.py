from .base import SlicerElement
from .filters import ComparatorFilter


class Metric(SlicerElement):
    """
    The `Metric` class represents a metric in the `Slicer` object.
    """

    def __init__(self, key, definition, label=None, precision=None, prefix=None, suffix=None):
        super(Metric, self).__init__(key, label, definition)
        self.precision = precision
        self.prefix = prefix
        self.suffix = suffix

    def __eq__(self, other):
        return ComparatorFilter(self.definition, ComparatorFilter.Operator.eq, other)

    def __ne__(self, other):
        return ComparatorFilter(self.definition, ComparatorFilter.Operator.ne, other)

    def __gt__(self, other):
        return ComparatorFilter(self.definition, ComparatorFilter.Operator.gt, other)

    def __ge__(self, other):
        return ComparatorFilter(self.definition, ComparatorFilter.Operator.gte, other)

    def __lt__(self, other):
        return ComparatorFilter(self.definition, ComparatorFilter.Operator.lt, other)

    def __le__(self, other):
        return ComparatorFilter(self.definition, ComparatorFilter.Operator.lte, other)
