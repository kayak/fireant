from collections import Iterable
from enum import Enum
from functools import wraps

from fireant.utils import (
    immutable,
)
from .filters import (
    AntiPatternFilter,
    BooleanFilter,
    ComparatorFilter,
    ContainsFilter,
    ExcludesFilter,
    PatternFilter,
    RangeFilter,
)


class DataType(Enum):
    date = 1
    text = 2
    number = 3
    boolean = 4

    def __repr__(self):
        return self.name


CONTINUOUS_TYPES = [DataType.number, DataType.date]
DISCRETE_TYPES = [DataType.text, DataType.boolean]


class DataSetFilterException(Exception):
    def __init__(self, msg, allowed):
        super().__init__(msg)
        self.allowed = allowed


def restrict_types(allowed):
    def wrapper(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            if self.data_type not in allowed:
                msg = '`{func}` filter can only be used on fields with the following data types: {types}.' \
                    .format(func=func.__name__,
                            types=", ".join(map(repr, allowed)))
                raise DataSetFilterException(msg, allowed)
            return func(self, *args, **kwargs)

        return inner

    return wrapper


class Field:
    """
    The `Field` class represents a field in the `DataSet` object. A field is a mapping to a column in a database query.

    :param alias:
        A unique identifier used to identify the metric when writing slicer queries. This value must be unique over
        the metrics in the slicer.

    :param definition:
        A pypika expression which is used to select the value when building SQL queries. For metrics, this query
        **must** be aggregated, since queries always use a ``GROUP BY`` clause an metrics are not used as a group.

    :param data_type: {Number, Text, Boolean, Date}
        When True, the field's definition should be treated as an aggregate expression.

    :param label: (optional)
        A display value used for the metric. This is used for rendering the labels within the visualizations. If not
        set, the alias will be used as the default.

    :param is_aggregate: (default: False)
        When True, the field's definition should be treated as an aggregate expression.

    :param precision: (optional)
        A precision value for rounding decimals. By default, no rounding will be applied.

    :param prefix: (optional)
        A prefix for rendering labels in visualizations such as '$'

    :param suffix:
        A suffix for rendering labels in visualizations such as '€'
    """

    def __init__(self,
                 alias,
                 definition,
                 data_type: DataType = DataType.number,
                 label=None,
                 prefix: str = None,
                 suffix: str = None,
                 thousands: str = None,
                 precision: int = None,
                 hyperlink_template: str = None):
        self.alias = alias
        self.data_type = data_type
        self.definition = definition
        self.label = label \
            if label is not None \
            else alias
        self.prefix = prefix
        self.suffix = suffix
        self.thousands = thousands
        self.precision = precision
        self.hyperlink_template = hyperlink_template

    def eq(self, other):
        return ComparatorFilter(self.alias, self.definition, ComparatorFilter.Operator.eq, other)

    def ne(self, other):
        return ComparatorFilter(self.alias, self.definition, ComparatorFilter.Operator.ne, other)

    @restrict_types(CONTINUOUS_TYPES)
    def gt(self, other):
        return ComparatorFilter(self.alias, self.definition, ComparatorFilter.Operator.gt, other)

    @restrict_types(CONTINUOUS_TYPES)
    def ge(self, other):
        return ComparatorFilter(self.alias, self.definition, ComparatorFilter.Operator.gte, other)

    @restrict_types(CONTINUOUS_TYPES)
    def lt(self, other):
        return ComparatorFilter(self.alias, self.definition, ComparatorFilter.Operator.lt, other)

    @restrict_types(CONTINUOUS_TYPES)
    def le(self, other):
        return ComparatorFilter(self.alias, self.definition, ComparatorFilter.Operator.lte, other)

    @restrict_types(CONTINUOUS_TYPES)
    def between(self, lower, upper):
        """
        Creates a filter to filter a slicer query.

        :param lower:
            The start time of the filter. This is the beginning of the window for which results should be included.
        :param upper:
            The stop time of the filter. This is the end of the window for which results should be included.
        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is between the values
            start and stop.
        """
        return RangeFilter(self.alias, self.definition, lower, upper)

    def isin(self, values: Iterable):
        """
        Creates a filter that filters to rows where

        :param values:
            An iterable of value to constrain the slicer query results by.

        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is one of a set of
            values. Opposite of #notin.
        """
        return ContainsFilter(self.alias, self.definition, values)

    def notin(self, values):
        """
        Creates a filter to filter a slicer query.

        :param values:
            An iterable of value to constrain the slicer query results by.

        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is *not* one of a set of
            values. Opposite of #isin.
        """
        return ExcludesFilter(self.alias, self.definition, values)

    @restrict_types([DataType.text])
    def like(self, pattern, *patterns):
        return PatternFilter(self.alias, self.definition, pattern, *patterns)

    @restrict_types([DataType.text])
    def not_like(self, pattern, *patterns):
        return AntiPatternFilter(self.alias, self.definition, pattern, *patterns)

    @restrict_types([DataType.boolean])
    def is_(self, value: bool):
        """
        Creates a filter to filter a slicer query.

        :param value:
            True or False
        :return:
            A slicer query filter used to filter a slicer query to results where this dimension is True or False.
        """
        return BooleanFilter(self.alias, self.definition, value)

    def __eq__(self, other):
        return self.eq(other)

    def __ne__(self, other):
        return self.ne(other)

    def __gt__(self, other):
        return self.gt(other)

    def __ge__(self, other):
        return self.ge(other)

    def __lt__(self, other):
        return self.lt(other)

    def __le__(self, other):
        return self.le(other)

    def __repr__(self):
        return "{alias}[{data_type}]".format(alias=self.alias,
                                             data_type=self.data_type)

    def __hash__(self):
        return hash(repr(self))

    @property
    @immutable
    def share(self):
        self._share = True
        return self
