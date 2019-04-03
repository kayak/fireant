from functools import partial

from .modifiers import DimensionModifier


class NumericInterval(DimensionModifier):
    def __init__(self, dimension, size=1, offset=0):
        self.size = size
        self.offset = offset
        super().__init__(dimension)

    def __eq__(self, other):
        return all([isinstance(other, NumericInterval),
                    self.size == other.size,
                    self.offset == other.offset])

    def __hash__(self):
        return hash(repr(self))


DATETIME_INTERVALS = ('hour', 'day', 'week', 'month', 'quarter', 'year')


class DatetimeInterval(DimensionModifier):
    def __init__(self, dimension, interval_key):
        super().__init__(dimension)
        self.interval_key = interval_key

    def __eq__(self, other):
        return isinstance(other, DatetimeInterval) \
               and self.alias == other.alias

    def __repr__(self):
        wrapped_key = super().__getattribute__('wrapped_key')
        wrapped = super().__getattribute__(wrapped_key)
        return '{}({})'.format(self.interval_key, repr(wrapped))

    def __hash__(self):
        return hash(repr(self))


hour, day, week, month, quarter, year = [partial(DatetimeInterval, interval_key=key)
                                         for key in DATETIME_INTERVALS]
