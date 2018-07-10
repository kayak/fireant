from pypika import functions as fn
from pypika.queries import QueryBuilder
from fireant import utils


class Reference(object):
    def __init__(self, dimension, reference_type, delta=False, delta_percent=False):
        self.dimension = dimension

        self.reference_type = reference_type
        self.key = reference_type.key + '_delta_percent' \
            if delta_percent \
            else reference_type.key + '_delta' \
            if delta \
            else reference_type.key

        self.label = reference_type.label + ' Δ%' \
            if delta_percent \
            else reference_type.label + ' Δ' \
            if delta \
            else reference_type.label

        self.time_unit = reference_type.time_unit
        self.interval = reference_type.interval

        self.delta = delta_percent or delta
        self.delta_percent = delta_percent

    def __eq__(self, other):
        return isinstance(self, Reference) \
               and self.dimension == other.dimension \
               and self.key == other.key

    def __hash__(self):
        return hash('reference{}{}'.format(self.key, self.dimension))

    def __repr__(self):
        return '{}({})'.format(self.key, self.dimension.key)


class ReferenceType(object):
    def __init__(self, key, label, time_unit: str, interval: int):
        self.key = key
        self.label = label

        self.time_unit = time_unit
        self.interval = interval

    def __call__(self, dimension, delta=False, delta_percent=False):
        return Reference(dimension, self, delta=delta, delta_percent=delta_percent)

    def __eq__(self, other):
        return isinstance(self, ReferenceType) \
               and self.time_unit == other.time_unit \
               and self.interval == other.interval

    def __hash__(self):
        return hash('reference' + self.key)


DayOverDay = ReferenceType('dod', 'DoD', 'day', 1)
WeekOverWeek = ReferenceType('wow', 'WoW', 'week', 1)
MonthOverMonth = ReferenceType('mom', 'MoM', 'month', 1)
QuarterOverQuarter = ReferenceType('qoq', 'QoQ', 'quarter', 1)
YearOverYear = ReferenceType('yoy', 'YoY', 'year', 1)


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


def reference_term(reference: Reference,
                   original_query: QueryBuilder,
                   ref_query: QueryBuilder):
    """
    Part of query building. Given a reference, the original slicer query, and the ref query, creates the pypika for
    the reference that should be selected in the reference container query.

    :param reference:
    :param original_query:
    :param ref_query:
    :return:
    """

    def original_field(metric):
        return original_query.field(utils.format_metric_key(metric.key))

    def ref_field(metric):
        return ref_query.field(utils.format_metric_key(metric.key))

    if reference.delta:
        if reference.delta_percent:
            return lambda metric: (original_field(metric) - ref_field(metric)) \
                                  * \
                                  (100 / fn.NullIf(ref_field(metric), 0))

        return lambda metric: original_field(metric) - ref_field(metric)

    return ref_field
