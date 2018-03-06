from typing import Iterable

from fireant.utils import immutable
from pypika.terms import (
    ValueWrapper,
    NullValue,
)
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

    def __init__(self, key, label=None, definition=None, display_definition=None):
        super(Dimension, self).__init__(key, label, definition, display_definition)
        self.is_rollup = False

    @immutable
    def rollup(self):
        """
        Configures this dimension and all subsequent dimensions in a slicer query to be rolled up to provide the totals.
        This will include an extra value for each pair of dimensions labeled `Totals`. which will include the totals for
        the group.
        """
        self.is_rollup = True


class BooleanDimension(Dimension):
    """
    This is a dimension that represents a boolean true/false value.  The expression should always result in a boolean
    value.
    """

    def __init__(self, key, label=None, definition=None):
        super(BooleanDimension, self).__init__(key=key,
                                               label=label,
                                               definition=definition)

    def is_(self, value: bool):
        """
        Creates a filter to filter a slicer query.

        :param value:
            True or False
        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is True or False.
        """
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

    def isin(self, values: Iterable):
        """
        Creates a filter to filter a slicer query.

        :param values:
            An iterable of value to constrain the slicer query results by.

        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is one of a set of
            values. Opposite of #notin.
        """
        return ContainsFilter(self.definition, values)

    def notin(self, values):
        """
        Creates a filter to filter a slicer query.

        :param values:
            An iterable of value to constrain the slicer query results by.

        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is *not* one of a set of
            values. Opposite of #isin.
        """
        return ExcludesFilter(self.definition, values)


class UniqueDimension(Dimension):
    """
    This is a dimension that represents a field in a database which is a unique identifier, such as a primary/foreign
    key. It provides support for a display value field which is selected and used in the results.
    """

    def __init__(self, key, label=None, definition=None, display_definition=None):
        super(UniqueDimension, self).__init__(key=key,
                                              label=label,
                                              definition=definition,
                                              display_definition=display_definition)


    def __hash__(self):
        if self.has_display_field:
            return hash('{}({},{})'.format(self.__class__.__name__, self.definition, self.display_definition))
        return super(UniqueDimension, self).__hash__()

    def isin(self, values, use_display=False):
        """
        Creates a filter to filter a slicer query.

        :param values:
            An iterable of value to constrain the slicer query results by.
        :param use_display:
            When True, the filter will be applied to the Dimesnion's display definition instead of the definition.

        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is one of a set of
            values. Opposite of #notin.
        """
        if use_display and self.display_definition is None:
            raise QueryException('No value set for display_definition.')
        filter_field = self.display_definition if use_display else self.definition
        return ContainsFilter(filter_field, values)

    def notin(self, values, use_display=False):
        """
        Creates a filter to filter a slicer query.

        :param values:
            An iterable of value to constrain the slicer query results by.
        :param use_display:
            When True, the filter will be applied to the Dimesnion's display definition instead of the definition.

        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is *not* one of a set of
            values. Opposite of #isin.
        """
        if use_display and self.display_definition is None:
            raise QueryException('No value set for display_definition.')
        filter_field = self.display_definition if use_display else self.definition
        return ExcludesFilter(filter_field, values)

    def wildcard(self, pattern):
        """
        Creates a filter to filter a slicer query.

        :param pattern:
            A pattern to match against the dimension's display definition.  This pattern is used in the SQL query as
            the
            `LIKE` expression.
        :return:
            A slicer query filter used to filter a slicer query to results where this dimension's display definition
            matches the pattern.
        """
        if self.display_definition is None:
            raise QueryException('No value set for display_definition.')
        return WildcardFilter(self.display_definition, pattern)

    @property
    def display(self):
        return self


class DisplayDimension(Dimension):
    """
    WRITEME
    """

    def __init__(self, dimension):
        super(DisplayDimension, self).__init__(dimension.display_key, dimension.label, dimension.display_definition)


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
        """
        When calling a datetime dimension an interval can be supplied:

        ```
        from fireant import weekly

        my_slicer.dimensions.date # Daily interval used as default
        my_slicer.dimensions.date(weekly) # Daily interval used as default
        ```

        :param interval:
            An interval to use with the dimension.  See `fireant.intervals`.
        :return:
            A copy of the dimension with the interval set.
        """
        self.interval = interval

    @immutable
    def reference(self, reference):
        """
        Add a reference to this dimension when building a slicer query.

        :param reference:
            A reference to add to the query
        :return:
            A copy of the dimension with the reference added.
        """
        self.references.append(reference)

    def between(self, start, stop):
        """
        Creates a filter to filter a slicer query.

        :param start:
            The start time of the filter. This is the beginning of the window for which results should be included.
        :param stop:
            The stop time of the filter. This is the end of the window for which results should be included.
        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is between the values
            start and stop.
        """
        return RangeFilter(self.definition, start, stop)


class TotalsDimension(Dimension):
    def __init__(self, dimension):
        totals_definition = NullValue()
        display_definition = totals_definition if dimension.has_display_field else None
        super(Dimension, self).__init__(dimension.key, dimension.label, totals_definition, display_definition)
