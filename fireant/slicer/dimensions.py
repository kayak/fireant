from fireant.utils import immutable

from .base import SlicerElement
from .exceptions import QueryException
from .filters import (
    BooleanFilter,
    ContainsFilter,
    ExcludesFilter,
    RangeFilter,
    WildcardFilter,
)
from .intervals import (
    NumericInterval,
    daily,
)


class Dimension(SlicerElement):
    """
    The `Dimension` class represents a dimension in the `Slicer` object.
    """

    def __init__(self, key, label=None, definition=None):
        super(Dimension, self).__init__(key, label, definition)


class BooleanDimension(Dimension):
    """
    This is a dimension that represents a boolean true/false value.  The expression should always result in a boolean
    value.
    """

    def __init__(self, key, label=None, definition=None):
        super(BooleanDimension, self).__init__(key=key,
                                               label=label,
                                               definition=definition)

    def is_(self, value):
        return BooleanFilter(self.definition, value)


class CategoricalDimension(Dimension):
    """
    This is a dimension that represents an enum-like database field, with a finite list of options to chose from. It
    provides support for configuring a display value for each of the possible values.
    """

    def __init__(self, key, label=None, definition=None, display_values=()):
        super(CategoricalDimension, self).__init__(key=key,
                                                   label=label,
                                                   definition=definition)
        self.display_values = dict(display_values)

    def isin(self, values):
        return ContainsFilter(self.definition, values)

    def notin(self, values):
        return ExcludesFilter(self.definition, values)


class UniqueDimension(Dimension):
    """
    This is a dimension that represents a field in a database which is a unique identifier, such as a primary/foreign
    key. It provides support for a display value field which is selected and used in the results.
    """

    def __init__(self, key, label=None, definition=None, display_definition=None):
        super(UniqueDimension, self).__init__(key=key, label=label, definition=definition)
        self.display_key = '{}_display'.format(key)
        self.display_definition = display_definition

    def isin(self, values, use_display=False):
        if use_display and self.display_definition is None:
            raise QueryException('No value set for display_definition.')
        filter_field = self.display_definition if use_display else self.definition
        return ContainsFilter(filter_field, values)

    def notin(self, values, use_display=False):
        if use_display and self.display_definition is None:
            raise QueryException('No value set for display_definition.')
        filter_field = self.display_definition if use_display else self.definition
        return ExcludesFilter(filter_field, values)

    def wildcard(self, pattern):
        if self.display_definition is None:
            raise QueryException('No value set for display_definition.')
        return WildcardFilter(self.display_definition, pattern)


class ContinuousDimension(Dimension):
    """
    This is a dimension that represents a field in the database which is a continuous value, such as a decimal, integer,
    or date/time. It requires the use of an interval which is the window over the values.
    """

    def __init__(self, key, label=None, definition=None, default_interval=NumericInterval(1, 0)):
        super(ContinuousDimension, self).__init__(key=key,
                                                  label=label,
                                                  definition=definition)
        self.interval = default_interval


class DatetimeDimension(ContinuousDimension):
    """
    A subclass of ContinuousDimension which reflects a date/time data type. Intervals are replaced with time intervals
    such as daily, weekly, annually, etc.  A reference can be used to show a comparison over time such as
    week-over-week or month-over-month.
    """

    def __init__(self, key, label=None, definition=None, default_interval=daily):
        super(DatetimeDimension, self).__init__(key=key,
                                                label=label,
                                                definition=definition,
                                                default_interval=default_interval)
        self.references = []

    @immutable
    def __call__(self, interval):
        self.interval = interval

    @immutable
    def reference(self, reference):
        self.references.append(reference)

    def between(self, start, stop):
        return RangeFilter(self.definition, start, stop)


class DimensionValue(object):
    """
    An option belongs to a categorical dimension which specifies a fixed set of values
    """

    def __init__(self, key, label=None):
        self.key = key
        self.label = label or key
