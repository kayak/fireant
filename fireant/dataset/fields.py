from collections import Iterable
from enum import Enum
from functools import wraps

from pypika.enums import Arithmetic
from pypika.terms import (
    ArithmeticExpression,
    Mod,
    Node,
    Pow,
)
from .filters import (
    AntiPatternFilter,
    BooleanFilter,
    ComparatorFilter,
    ContainsFilter,
    ExcludesFilter,
    PatternFilter,
    RangeFilter,
    VoidFilter,
)
from ..utils import immutable


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
                msg = "`{func}` filter can only be used on fields with the following data types: {types}.".format(
                    func=func.__name__, types=", ".join(map(repr, allowed))
                )
                raise DataSetFilterException(msg, allowed)
            return func(self, *args, **kwargs)

        return inner

    return wrapper


class Field(Node):
    """
    The `Field` class represents a field in the `DataSet` object. A field is a mapping to a column in a database query.

    :param alias:
        A unique identifier used to identify the metric when writing dataset queries. This value must be unique over
        the metrics in the dataset.

    :param definition:
        A pypika expression which is used to select the value when building SQL queries. For metrics, this query
        **must** be aggregated, since queries always use a ``GROUP BY`` clause an metrics are not used as a group.

    :param data_type: {Number, Text, Boolean, Date}
        When True, the field's definition should be treated as an aggregate expression.

    :param label: (optional)
        A display value used for the metric. This is used for rendering the labels within the visualizations. If not
        set, the alias will be used as the default.

    :param hint_table: (optional)
        An optional table for fetching field choices. If this table is not set, the base table from the field
        definition will be used.

    :param precision: (optional)
        A precision value for rounding decimals. By default, no rounding will be applied.

    :param prefix: (optional)
        A prefix for rendering labels in visualizations such as '$'

    :param suffix:
        A suffix for rendering labels in visualizations such as '€'
    """

    def __init__(
        self,
        alias,
        definition,
        data_type: DataType = DataType.number,
        label=None,
        hint_table=None,
        prefix: str = None,
        suffix: str = None,
        thousands: str = None,
        precision: int = None,
        hyperlink_template: str = None,
    ):
        self.alias = alias
        self.data_type = data_type
        self.definition = definition
        self.label = label if label is not None else alias
        self.hint_table = hint_table
        self.prefix = prefix
        self.suffix = suffix
        self.thousands = thousands
        self.precision = precision
        self.hyperlink_template = hyperlink_template

    @property
    def is_aggregate(self):
        return self.definition.is_aggregate

    def eq(self, other):
        return ComparatorFilter(self, ComparatorFilter.Operator.eq, other)

    def ne(self, other):
        return ComparatorFilter(self, ComparatorFilter.Operator.ne, other)

    @restrict_types(CONTINUOUS_TYPES)
    def gt(self, other):
        return ComparatorFilter(self, ComparatorFilter.Operator.gt, other)

    @restrict_types(CONTINUOUS_TYPES)
    def ge(self, other):
        return ComparatorFilter(self, ComparatorFilter.Operator.gte, other)

    @restrict_types(CONTINUOUS_TYPES)
    def lt(self, other):
        return ComparatorFilter(self, ComparatorFilter.Operator.lt, other)

    @restrict_types(CONTINUOUS_TYPES)
    def le(self, other):
        return ComparatorFilter(self, ComparatorFilter.Operator.lte, other)

    @restrict_types(CONTINUOUS_TYPES)
    def between(self, lower, upper):
        """
        Creates a filter to filter a dataset query.

        :param lower:
            The start time of the filter. This is the beginning of the window for which results should be included.
        :param upper:
            The stop time of the filter. This is the end of the window for which results should be included.
        :return:
            A dataset query filter used to filter a dataset query to results where this dimension is between the values
            start and stop.
        """
        return RangeFilter(self, lower, upper)

    def isin(self, values: Iterable):
        """
        Creates a filter that filters to rows where

        :param values:
            An iterable of value to constrain the dataset query results by.

        :return:
            A dataset query filter used to filter a dataset query to results where this dimension is one of a set of
            values. Opposite of #notin.
        """
        return ContainsFilter(self, values)

    def notin(self, values):
        """
        Creates a filter to filter a dataset query.

        :param values:
            An iterable of value to constrain the dataset query results by.

        :return:
            A dataset query filter used to filter a dataset query to results where this dimension is *not* one of a
            set of
            values. Opposite of #isin.
        """
        return ExcludesFilter(self, values)

    @restrict_types([DataType.text])
    def like(self, pattern, *patterns):
        return PatternFilter(self, pattern, *patterns)

    @restrict_types([DataType.text])
    def not_like(self, pattern, *patterns):
        return AntiPatternFilter(self, pattern, *patterns)

    @restrict_types([DataType.boolean])
    def is_(self, value: bool):
        """
        Creates a filter to filter a dataset query.

        :param value:
            True or False
        :return:
            A dataset query filter used to filter a dataset query to results where this dimension is True or False.
        """
        return BooleanFilter(self, value)

    def void(self):
        return VoidFilter(self)

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

    def __add__(self, other):
        return ArithmeticExpression(Arithmetic.add, self, other)

    def __sub__(self, other):
        return ArithmeticExpression(Arithmetic.sub, self, other)

    def __mul__(self, other):
        return ArithmeticExpression(Arithmetic.mul, self, other)

    def __truediv__(self, other):
        return ArithmeticExpression(Arithmetic.div, self, other)

    def __pow__(self, other):
        return Pow(self, other)

    def __mod__(self, other):
        return Mod(self, other)

    def __radd__(self, other):
        return self.__add__(other)

    def __rsub__(self, other):
        return self.__sub__(other)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __rtruediv__(self, other):
        return self.__truediv__(other)

    def __repr__(self):
        return "{alias}[{data_type}]".format(alias=self.alias, data_type=self.data_type)

    def __hash__(self):
        if not hasattr(self, "alias"):
            """
            Python's deep copy built-in are a problem when:
                Have classes that must be hashed and contain reference cycles, and
                Don't ensure hash-related (and equality related) invariants are established at object
                construction, not just initialization
            Since that's our case, we do this simple check to avoid exceptions during deep copy. Note that any
            of the variables in this class would amount to the same. That said it's best to use a non nullable
            positional argument.
            """
            return hash(None)

        return id(self)

    def for_(self, replacement):
        return replacement

    def get_sql(self, **kwargs):
        raise NotImplementedError

    @property
    @immutable
    def share(self):
        self._share = True
        return self
