from pypika import Not
from pypika.functions import Lower


class Filter(object):
    def __init__(self, definition):
        self.definition = definition

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
               and str(self.definition) == str(other.definition)

    def __repr__(self):
        return str(self.definition)


class DimensionFilter(Filter):
    def __init__(self, dimension_key, definition):
        super().__init__(definition)
        self.dimension_key = dimension_key


class MetricFilter(Filter):
    def __init__(self, metric_key, definition):
        super().__init__(definition)
        self.metric_key = metric_key


class ComparatorFilter(MetricFilter):
    class Operator(object):
        eq = 'eq'
        ne = 'ne'
        gt = 'gt'
        lt = 'lt'
        gte = 'gte'
        lte = 'lte'

    def __init__(self, metric_key, metric_definition, operator, value):
        definition = getattr(metric_definition, operator)(value)
        super(ComparatorFilter, self).__init__(metric_key, definition)


class BooleanFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, value):
        definition = dimension_definition \
            if value \
            else Not(dimension_definition)

        super(BooleanFilter, self).__init__(dimension_key, definition)


class ContainsFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, values):
        definition = dimension_definition.isin(values)
        super(ContainsFilter, self).__init__(dimension_key, definition)


class ExcludesFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, values):
        definition = dimension_definition.notin(values)
        super(ExcludesFilter, self).__init__(dimension_key, definition)


class RangeFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, start, stop):
        definition = dimension_definition[start:stop]
        super(RangeFilter, self).__init__(dimension_key, definition)


class PatternFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, pattern, *patterns):
        definition = self._apply(dimension_definition, (pattern,) + patterns)
        super(PatternFilter, self).__init__(dimension_key, definition)

    def _apply(self, dimension_definition, patterns):
        definition = Lower(dimension_definition).like(Lower(patterns[0]))

        for pattern in patterns[1:]:
            definition |= Lower(dimension_definition).like(Lower(pattern))

        return definition


class AntiPatternFilter(PatternFilter):
    def _apply(self, dimension_definition, pattern):
        return super(AntiPatternFilter, self)._apply(dimension_definition, pattern).negate()
