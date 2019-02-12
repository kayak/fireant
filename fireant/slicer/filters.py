from pypika import Not
from pypika.functions import Lower


class Filter(object):
    def __init__(self, definition, apply_to_totals):
        self.definition = definition
        self.apply_to_totals = apply_to_totals

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
               and str(self.definition) == str(other.definition)

    def __repr__(self):
        return str(self.definition)


class DimensionFilter(Filter):
    def __init__(self, dimension_key, definition, apply_to_totals):
        super().__init__(definition, apply_to_totals)
        self.dimension_key = dimension_key


class MetricFilter(Filter):
    def __init__(self, metric_key, definition, apply_to_totals):
        super().__init__(definition, apply_to_totals)
        self.metric_key = metric_key


class ComparatorFilter(MetricFilter):
    class Operator(object):
        eq = 'eq'
        ne = 'ne'
        gt = 'gt'
        lt = 'lt'
        gte = 'gte'
        lte = 'lte'

    def __init__(self, metric_key, metric_definition, operator, value, apply_to_totals):
        definition = getattr(metric_definition, operator)(value)
        super(ComparatorFilter, self).__init__(metric_key, definition, apply_to_totals)


class BooleanFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, value, apply_to_totals):
        definition = dimension_definition \
            if value \
            else Not(dimension_definition)

        super(BooleanFilter, self).__init__(dimension_key, definition, apply_to_totals)


class ContainsFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, values, apply_to_totals):
        definition = dimension_definition.isin(values)
        super(ContainsFilter, self).__init__(dimension_key, definition, apply_to_totals)


class ExcludesFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, values, apply_to_totals):
        definition = dimension_definition.notin(values)
        super(ExcludesFilter, self).__init__(dimension_key, definition, apply_to_totals)


class RangeFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, start, stop, apply_to_totals):
        definition = dimension_definition[start:stop]
        super(RangeFilter, self).__init__(dimension_key, definition, apply_to_totals)


class PatternFilter(DimensionFilter):
    def __init__(self, dimension_key, dimension_definition, pattern, *patterns, apply_to_totals):
        definition = self._apply(dimension_definition, (pattern,) + patterns)
        super(PatternFilter, self).__init__(dimension_key, definition, apply_to_totals)

    def _apply(self, dimension_definition, patterns):
        definition = Lower(dimension_definition).like(Lower(patterns[0]))

        for pattern in patterns[1:]:
            definition |= Lower(dimension_definition).like(Lower(pattern))

        return definition


class AntiPatternFilter(PatternFilter):
    def _apply(self, dimension_definition, pattern):
        return super(AntiPatternFilter, self)._apply(dimension_definition, pattern).negate()
