# coding: utf8


def wrap_list(value):
    return value if isinstance(value, (tuple, list)) else [value]


class Filter(object):
    def __init__(self, element_key):
        self.element_key = element_key


class EqualityFilter(Filter):
    def __init__(self, element_key, operator, value):
        super(EqualityFilter, self).__init__(element_key)
        self.operator = operator
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, EqualityFilter):
            return False

        return self.element_key == other.element_key and self.operator == other.operator and self.value == other.value

    def __hash__(self):
        return hash('%s_%s_%s' % (self.element_key, self.operator, self.value))

    def schemas(self, element):
        element = wrap_list(element)
        value = wrap_list(self.value)

        criterion = None
        for el, value in zip(element, value):
            crit = getattr(el, self.operator)(value)
            if criterion:
                criterion = criterion & crit
            else:
                criterion = crit

        return criterion


class ContainsFilter(Filter):
    def __init__(self, element_key, values):
        super(ContainsFilter, self).__init__(element_key)
        self.values = values

    def __eq__(self, other):
        if not isinstance(other, ContainsFilter):
            return False

        return self.element_key == other.element_key and self.values == other.values

    def __hash__(self):
        return hash('%s_%s_%s' % (self.__class__, self.element_key, self.values))

    def schemas(self, element):
        return element.isin(self.values)


class RangeFilter(Filter):
    def __init__(self, element_key, start, stop):
        super(RangeFilter, self).__init__(element_key)
        self.start = start
        self.stop = stop

    def __eq__(self, other):
        if not isinstance(other, RangeFilter):
            return False

        return self.element_key == other.element_key and self.start == other.start and self.stop == other.stop

    def __hash__(self):
        return hash('%s_%s_%s_%s' % (self.__class__, self.element_key, self.start, self.stop))

    def schemas(self, element):
        element = wrap_list(element)
        starts = wrap_list(self.start)
        stops = wrap_list(self.stop)

        criterion = None
        for el, start, stop in zip(element, starts, stops):
            crit = el[start:stop]
            if criterion:
                criterion = criterion & crit
            else:
                criterion = crit

        return criterion


class WildcardFilter(Filter):
    def __init__(self, element_key, value):
        super(WildcardFilter, self).__init__(element_key)
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, EqualityFilter):
            return False

        return self.element_key == other.element_key and self.value == other.value

    def schemas(self, element):
        return element.like(self.value)

    def __hash__(self):
        return hash('%s_%s_%s' % (self.__class__, self.element_key, self.value))
