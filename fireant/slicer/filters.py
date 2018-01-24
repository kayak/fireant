from pypika import Not


class Filter(object):
    def __init__(self, definition):
        self.definition = definition

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
               and str(self.definition) == str(other.definition)

    def __repr__(self):
        return str(self.definition)


class DimensionFilter(Filter):
    pass


class MetricFilter(Filter):
    pass


class ComparatorFilter(MetricFilter):
    class Operator(object):
        eq = 'eq'
        ne = 'ne'
        gt = 'gt'
        lt = 'lt'
        gte = 'gte'
        lte = 'lte'

    def __init__(self, metric_definition, operator, value):
        definition = getattr(metric_definition, operator)(value)
        super(ComparatorFilter, self).__init__(definition)


class BooleanFilter(DimensionFilter):
    def __init__(self, element_key, value):
        definition = element_key if value else Not(element_key)
        super(BooleanFilter, self).__init__(definition)


class ContainsFilter(DimensionFilter):
    def __init__(self, dimension_definition, values):
        definition = dimension_definition.isin(values)
        super(ContainsFilter, self).__init__(definition)


class ExcludesFilter(DimensionFilter):
    def __init__(self, dimension_definition, values):
        definition = dimension_definition.notin(values)
        super(ExcludesFilter, self).__init__(definition)


class RangeFilter(DimensionFilter):
    def __init__(self, dimension_definition, start, stop):
        definition = dimension_definition[start:stop]
        super(RangeFilter, self).__init__(definition)


class WildcardFilter(DimensionFilter):
    def __init__(self, dimension_definition, pattern):
        definition = dimension_definition.like(pattern)
        super(WildcardFilter, self).__init__(definition)
