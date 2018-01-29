from fireant import (
    Dimension,
    Metric,
)
from pypika.queries import QueryBuilder


class PypikaQueryMatcher:
    def __init__(self, query_str):
        self.query_str = query_str

    def __eq__(self, other):
        return isinstance(other, QueryBuilder) \
               and self.query_str == str(other)

    def __repr__(self):
        return self.query_str


class _ElementsMatcher:
    expected_class = None

    def __init__(self, *elements):
        self.elements = elements

    def __eq__(self, other):
        return all([isinstance(actual, self.expected_class)
                    and expected.key == actual.key
                    for expected, actual in zip(self.elements, other)])

    def __repr__(self):
        return '[{}]'.format(','.join(str(element)
                                      for element in self.elements))


class MetricMatcher(_ElementsMatcher):
    expected_class = Metric


class DimensionMatcher(_ElementsMatcher):
    expected_class = Dimension
