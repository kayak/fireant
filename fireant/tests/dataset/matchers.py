from fireant import Field


class PypikaQueryMatcher:
    def __init__(self, query_str):
        self.query_str = query_str

    def __eq__(self, other):
        return str(self.query_str) == str(other)

    def __repr__(self):
        return repr(self.query_str)


class _ElementsMatcher:
    expected_class = None

    def __init__(self, *elements):
        self.elements = elements

    def __eq__(self, other):
        return all(
            [
                isinstance(actual, self.expected_class) and expected.alias == actual.alias
                for expected, actual in zip(self.elements, other)
            ]
        )

    def __repr__(self):
        return '[{}]'.format(','.join(str(element) for element in self.elements))


class FieldMatcher(_ElementsMatcher):
    expected_class = Field
