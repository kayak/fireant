# coding: utf8
from fireant import utils


class Filter(object):
    def __init__(self, element_key):
        self.element_key = element_key


class EqualityFilter(Filter):
    def __init__(self, element_key, operator, value):
        super(EqualityFilter, self).__init__(element_key)
        self.operator = operator
        self.value = value

    def schemas(self, elements, **kwargs):
        elements = utils.wrap_list(elements)
        value = utils.wrap_list(self.value)

        criterion = None
        for element, value in zip(elements, value):
            crit = getattr(element, self.operator)(value)
            if criterion:
                criterion = criterion & crit
            else:
                criterion = crit

        return criterion


class ContainsFilter(Filter):
    def __init__(self, element_key, values):
        super(ContainsFilter, self).__init__(element_key)
        self.values = values

    def schemas(self, element):
        return element.isin(self.values)


class ExcludesFilter(Filter):
    def __init__(self, element_key, values):
        super(ExcludesFilter, self).__init__(element_key)
        self.values = values

    def schemas(self, element):
        return element.notin(self.values)


class RangeFilter(Filter):
    def __init__(self, element_key, start, stop):
        super(RangeFilter, self).__init__(element_key)
        self.start = start
        self.stop = stop

    def schemas(self, element):
        element = utils.wrap_list(element)
        starts = utils.wrap_list(self.start)
        stops = utils.wrap_list(self.stop)

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

    def schemas(self, element):
        return element.like(self.value)
