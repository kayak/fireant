class NumericInterval(object):
    def __init__(self, size=1, offset=0):
        self.size = size
        self.offset = offset

    def __eq__(self, other):
        return all([isinstance(other, NumericInterval),
                    self.size == other.size,
                    self.offset == other.offset])

    def __hash__(self):
        return hash('NumericInterval %d %d' % (self.size, self.offset))

    def __repr__(self):
        return 'NumericInterval(size=%d,offset=%d)' % (self.size, self.offset)


class DatetimeInterval(object):
    def __init__(self, key):
        self.key = key

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, DatetimeInterval) \
               and self.key == other.key

    def __str__(self):
        return self.key

    def __repr__(self):
        return 'DatetimeInterval(\'{}\')'.format(self.key)


hourly = DatetimeInterval('hour')
daily = DatetimeInterval('day')
weekly = DatetimeInterval('week')
monthly = DatetimeInterval('month')
quarterly = DatetimeInterval('quarter')
annually = DatetimeInterval('year')
