class Reference(object):
    def __init__(self, key, label, time_unit: str, interval: int, delta=False, percent=False):
        self.key = key
        self.label = label

        self.time_unit = time_unit
        self.interval = interval

        self.is_delta = delta
        self.is_percent = percent

    def delta(self, percent=False):
        key = self.key + '_delta_percent' if percent else self.key + '_delta'
        label = self.label + ' Δ%' if percent else self.label + ' Δ'
        return Reference(key, label, self.time_unit, self.interval, delta=True, percent=percent)


DayOverDay = Reference('dod', 'DoD', 'day', 1)
WeekOverWeek = Reference('wow', 'WoW', 'week', 1)
MonthOverMonth = Reference('mom', 'MoM', 'month', 1)
QuarterOverQuarter = Reference('qoq', 'QoQ', 'quarter', 1)
YearOverYear = Reference('yoy', 'YoY', 'year', 1)
