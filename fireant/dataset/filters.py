from pypika import (
    EmptyCriterion,
    Not,
)
from pypika.functions import Lower

from .modifiers import FieldModifier


class ComparisonOperator:
    eq = "eq"
    ne = "ne"
    gt = "gt"
    lt = "lt"
    gte = "gte"
    lte = "lte"

    mapping = {
        '=': eq,
        '!=': ne,
        '>': gt,
        '<': lt,
        '>=': gte,
        '<=': lte,
    }

    @staticmethod
    def eval(value_1, operator, value_2):
        if operator == ComparisonOperator.eq:
            return value_1 == value_2

        if operator == ComparisonOperator.ne:
            return value_1 != value_2

        if operator == ComparisonOperator.lt:
            return value_1 < value_2

        if operator == ComparisonOperator.lte:
            return value_1 <= value_2

        if operator == ComparisonOperator.gt:
            return value_1 > value_2

        if operator == ComparisonOperator.gte:
            return value_1 >= value_2


class Filter(FieldModifier):
    @property
    def definition(self):
        raise NotImplementedError()

    @property
    def is_aggregate(self):
        return self.definition.is_aggregate

    @property
    def is_wrapped(self) -> bool:
        """
        We do not wrap field modifiers e.g. filters, just fields themselves during data blending.
        This is for convenience as Filter and Field classes are often passed to util functions as part of the
        same collection.
        """
        return False

    def __eq__(self, other):
        return all(
            [
                isinstance(other, self.__class__),
                str(self.definition) == str(other.definition),
            ]
        )

    def __repr__(self):
        return str(self.definition)


class ComparatorFilter(Filter):

    def __init__(self, field, operator, value):
        self.operator = operator
        self.value = value
        super(ComparatorFilter, self).__init__(field)

    @property
    def definition(self):
        return getattr(self.field.definition, self.operator)(self.value)

    @property
    def is_aggregate(self):
        return self.field.is_aggregate


class BooleanFilter(Filter):
    def __init__(self, field, value):
        self.value = value
        super(BooleanFilter, self).__init__(field)

    @property
    def definition(self):
        if self.value:
            return self.field.definition
        return Not(self.field.definition)


class ContainsFilter(Filter):
    def __init__(self, field, values):
        self.values = values
        super(ContainsFilter, self).__init__(field)

    @property
    def definition(self):
        return self.field.definition.isin(self.values)


class NegatedFilterMixin:
    @property
    def definition(self):
        definition = super().definition
        return definition.negate()


class ExcludesFilter(NegatedFilterMixin, ContainsFilter):
    pass


class RangeFilter(Filter):
    def __init__(self, field, start, stop):
        self.start = start
        self.stop = stop
        super(RangeFilter, self).__init__(field)

    @property
    def definition(self):
        return self.field.definition[self.start:self.stop]


class PatternFilter(Filter):
    def __init__(self, field, pattern, *patterns):
        self.patterns = (pattern, *patterns)
        super(PatternFilter, self).__init__(field)

    @property
    def definition(self):
        first, *rest = self.patterns
        definition = Lower(self.field.definition).like(Lower(first))

        for pattern in rest:
            definition |= Lower(self.field.definition).like(Lower(pattern))

        return definition


class AntiPatternFilter(NegatedFilterMixin, PatternFilter):
    pass


class VoidFilter(Filter):
    def __init__(self, field):
        super(VoidFilter, self).__init__(field)

    @property
    def definition(self):
        return EmptyCriterion()

    def __repr__(self):
        return "VoidFilter()"
