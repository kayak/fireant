class Reference(object):
    def __init__(self, key, time_unit, interval, delta=False, percent=False):
        self.key = key
        if delta:
            self.key += '_delta'
            if percent:
                self.key += '_percent'

        self.time_unit = time_unit
        self.interval = interval

        self.is_delta = delta
        self.is_percent = percent

    def delta(self, percent=False):
        return Reference(self.key, self.time_unit, self.interval, delta=True, percent=percent)


DayOverDay = Reference('dod', 'day', 1)
WeekOverWeek = Reference('wow', 'week', 1)
MonthOverMonth = Reference('mom', 'month', 1)
QuarterOverQuarter = Reference('qoq', 'quarter', 1)
YearOverYear = Reference('yoy', 'year', 1)
