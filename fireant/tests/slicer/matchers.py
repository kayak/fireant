from fireant import (
    Dimension,
    Metric,
)


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
