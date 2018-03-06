from pypika import functions as fn
from pypika.queries import QueryBuilder


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

    def __eq__(self, other):
        return isinstance(self, Reference) \
               and self.time_unit == other.time_unit \
               and self.interval == other.interval \
               and self.is_delta == other.is_delta \
               and self.is_percent == other.is_percent

    def __hash__(self):
        return hash('reference' + self.key)


DayOverDay = Reference('dod', 'DoD', 'day', 1)
WeekOverWeek = Reference('wow', 'WoW', 'week', 1)
MonthOverMonth = Reference('mom', 'MoM', 'month', 1)
QuarterOverQuarter = Reference('qoq', 'QoQ', 'quarter', 1)
YearOverYear = Reference('yoy', 'YoY', 'year', 1)


def reference_term(reference: Reference,
                   original_query: QueryBuilder,
                   ref_query: QueryBuilder):
    """
    WRITEME

    :param reference:
    :param original_query:
    :param ref_query:
    :return:
    """

    def ref_field(metric):
        return ref_query.field(metric.key)

    if reference.is_delta:
        if reference.is_percent:
            return lambda metric: (original_query.field(metric.key) - ref_field(metric)) \
                                  * \
                                  (100 / fn.NullIf(ref_field(metric), 0))

        return lambda metric: original_query.field(metric.key) - ref_field(metric)

    return ref_field


def reference_key(metric, reference):
    """
    Format a metric key for a reference.

    :return:
        A string that is used as the key for a reference metric.
    """
    key = metric.key

    if reference is not None:
        return '{}_{}'.format(key, reference.key)

    return key


def reference_label(metric, reference):
    """
    Format a metric label for a reference.

    :return:
        A string that is used as the display value for a reference metric.
    """
    label = metric.label or metric.key

    if reference is not None:
        return '{} ({})'.format(label, reference.label)

    return label
