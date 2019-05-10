from pypika import Not
from pypika.functions import Lower


class Filter(object):
    def __init__(self, field_alias, definition):
        self.field_alias = field_alias
        self.definition = definition

    @property
    def is_aggregate(self):
        return self.definition.is_aggregate

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
               and str(self.definition) == str(other.definition)

    def __repr__(self):
        return str(self.definition)


class ComparatorFilter(Filter):
    class Operator(object):
        eq = 'eq'
        ne = 'ne'
        gt = 'gt'
        lt = 'lt'
        gte = 'gte'
        lte = 'lte'

    def __init__(self, field_alias, metric_definition, operator, value):
        definition = getattr(metric_definition, operator)(value)
        super(ComparatorFilter, self).__init__(field_alias, definition)


class BooleanFilter(Filter):
    def __init__(self, field_alias, dimension_definition, value):
        definition = dimension_definition \
            if value \
            else Not(dimension_definition)

        super(BooleanFilter, self).__init__(field_alias, definition)


class ContainsFilter(Filter):
    def __init__(self, field_alias, dimension_definition, values):
        definition = dimension_definition.isin(values)
        super(ContainsFilter, self).__init__(field_alias, definition)


class ExcludesFilter(Filter):
    def __init__(self, field_alias, dimension_definition, values):
        definition = dimension_definition.notin(values)
        super(ExcludesFilter, self).__init__(field_alias, definition)


class RangeFilter(Filter):
    def __init__(self, field_alias, dimension_definition, start, stop):
        definition = dimension_definition[start:stop]
        super(RangeFilter, self).__init__(field_alias, definition)


class PatternFilter(Filter):
    def __init__(self, field_alias, dimension_definition, pattern, *patterns):
        definition = self._apply(dimension_definition, (pattern,) + patterns)
        super(PatternFilter, self).__init__(field_alias, definition)

    def _apply(self, dimension_definition, patterns):
        definition = Lower(dimension_definition).like(Lower(patterns[0]))

        for pattern in patterns[1:]:
            definition |= Lower(dimension_definition).like(Lower(pattern))

        return definition


class AntiPatternFilter(PatternFilter):
    def _apply(self, dimension_definition, pattern):
        return super(AntiPatternFilter, self)._apply(dimension_definition, pattern).negate()
