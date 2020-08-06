from collections import Iterable
from datetime import datetime
from enum import Enum
from functools import wraps
from numbers import Number
from typing import Type, Union

from pypika import Field as PyPikaField
from pypika.enums import Arithmetic
from pypika.terms import (
    ArithmeticExpression,
    Function,
    Mod,
    Node,
    Pow,
)

from .filters import (
    AntiPatternFilter,
    BooleanFilter,
    ComparatorFilter,
    ComparisonOperator,
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
        A prefix for rendering labels in visualizations such as '$'.

    :param suffix: (optional)
        A suffix for rendering labels in visualizations such as '€'.

    :param thousands: (optional)
        A character to be used as a thousand separator when rendering values.

    :param hyperlink_template: (optional)
        A url string that can also reference other fields in a dataset. To reference a field put its alias
        between {}. This is only used in certain widgets (i.e. react table).

    :param fetch_only: (optional)
        Whether the field data should be ignored in widgets. This is useful for not displaying hyperlink
        dependencies, which might be necessary only for the purpose of generating a hyperlink and have no
        effect on the dimension grouping.
    """

    def __init__(
        self,
        alias: str,
        definition: Union[PyPikaField, Type[Function]],
        data_type: DataType = DataType.number,
        label=None,
        hint_table=None,
        prefix: str = None,
        suffix: str = None,
        thousands: str = None,
        precision: int = None,
        hyperlink_template: str = None,
        fetch_only: bool = False,
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
        self.fetch_only = fetch_only

        # An artificial field is created dynamically as the query is mounted, instead of being defined during
        # instantiation. That's the case for set dimensions, for instance. The only practical aspect of this
        # is that artificial dimensions are hashed differently (see dunder hash method). Instead of using the
        # memory reference, artificial dimensions use a set of its fields.
        self.is_artificial = False

    @property
    def is_aggregate(self):
        return self.definition.is_aggregate

    @property
    def is_wrapped(self) -> bool:
        """
        This allows calling code can easily tell whether the field has been wrapped.
        This occurs if data blending is being used.
        """
        return bool(getattr(self.definition, 'definition', False))

    def eq(self, other: Number) -> ComparatorFilter:
        return ComparatorFilter(self, ComparisonOperator.eq, other)

    def ne(self, other: Number) -> ComparatorFilter:
        return ComparatorFilter(self, ComparisonOperator.ne, other)

    @restrict_types(CONTINUOUS_TYPES)
    def gt(self, other: Number) -> ComparatorFilter:
        return ComparatorFilter(self, ComparisonOperator.gt, other)

    @restrict_types(CONTINUOUS_TYPES)
    def ge(self, other: Number) -> ComparatorFilter:
        return ComparatorFilter(self, ComparisonOperator.gte, other)

    @restrict_types(CONTINUOUS_TYPES)
    def lt(self, other: Number):
        return ComparatorFilter(self, ComparisonOperator.lt, other)

    @restrict_types(CONTINUOUS_TYPES)
    def le(self, other: Number) -> ComparatorFilter:
        return ComparatorFilter(self, ComparisonOperator.lte, other)

    @restrict_types(CONTINUOUS_TYPES)
    def between(self, lower: datetime, upper: datetime) -> RangeFilter:
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

    def isin(self, values: Iterable) -> ContainsFilter:
        """
        Creates a filter that filters to rows where

        :param values:
            An iterable of value to constrain the dataset query results by.

        :return:
            A dataset query filter used to filter a dataset query to results where this dimension is one of a set of
            values. Opposite of #notin.
        """
        return ContainsFilter(self, values)

    def notin(self, values: Iterable) -> ExcludesFilter:
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
    def like(self, pattern: str, *patterns: Iterable) -> PatternFilter:
        return PatternFilter(self, pattern, *patterns)

    @restrict_types([DataType.text])
    def not_like(self, pattern: str, *patterns: Iterable) -> AntiPatternFilter:
        return AntiPatternFilter(self, pattern, *patterns)

    @restrict_types([DataType.boolean])
    def is_(self, value: bool) -> BooleanFilter:
        """
        Creates a filter to filter a dataset query.

        :param value:
            True or False
        :return:
            A dataset query filter used to filter a dataset query to results where this dimension is True or False.
        """
        return BooleanFilter(self, value)

    def void(self) -> VoidFilter:
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

        if self.is_artificial:
            """
            Python's deep copy built-in are a problem when:
                Handling artificial fields (e.g. set field) in data blending.
            Given artificial fields are dynamically generated, they can't be properly compared when hashed
            by memory id. Therefore we hash them differently.
            """
            return hash((self.alias, self.label, self.data_type, self.definition))

        return id(self)

    def for_(self, replacement):
        return replacement

    def get_sql(self, *args, **kwargs) -> str:
        return self.definition.get_sql(*args, **kwargs)

    @property
    @immutable
    def share(self):
        self._share = True
        return self
