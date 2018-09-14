from .base import SlicerElement
from .filters import ComparatorFilter


class Metric(SlicerElement):
    """
    The `Metric` class represents a metric in the `Slicer` object.

    :param alias:
        A unique identifier used to identify the metric when writing slicer queries. This value must be unique over the metrics in the slicer.

    :param definition:
        A pypika expression which is used to select the value when building SQL queries. For metrics, this query **must** be aggregated, since queries always use a ``GROUP BY`` clause an metrics are not used as a group.

    :param label: (optional)
        A display value used for the metric. This is used for rendering the labels within the visualizations. If not set, the alias will be used as the default.

    :param precision: (optional)
        A precision value for rounding decimals. By default, no rounding will be applied.

    :param prefix: (optional)
        A prefix for rendering labels in visualizations such as '$'

    :param suffix:
        A suffix for rendering labels in visualizations such as '€'
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

    def __repr__(self):
        return "slicer.metrics.{}".format(self.key)
