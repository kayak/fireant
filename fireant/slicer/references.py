# coding: utf-8
from pypika import functions as fn


class Reference(object):
    key = None
    label = None
    modifier = None  # Modifier calculation function e.g. delta percentage arithmetic
    time_unit = None  # The time unit to perform the reference on - WoW is week, YoY is year etc.
    interval = None  # The number unit used in the interval e.g. for 1 week it is 1

    def __init__(self, element_key):
        self.element_key = element_key

    def __eq__(self, other):
        if not isinstance(other, Reference):
            return False

        return all([
            self.key == other.key,
            self.element_key == other.element_key,
        ])

    def __hash__(self):
        return hash('%s:%s' % (self.key, self.element_key))


class Delta(Reference):
    def __init__(self, reference):
        super(Delta, self).__init__(reference.element_key)
        self.key = self.generate_key(reference.key)
        self.label = '%s Δ' % reference.label
        self.time_unit = reference.time_unit
        self.interval = reference.interval

    @staticmethod
    def modifier(field, join_field):
        return field - join_field

    @staticmethod
    def generate_key(reference_key):
        return '%s_delta' % reference_key


class DeltaPercentage(Reference):
    def __init__(self, reference):
        super(DeltaPercentage, self).__init__(reference.element_key)
        self.key = self.generate_key(reference.key)
        self.label = '%s Δ%%' % reference.label
        self.time_unit = reference.time_unit
        self.interval = reference.interval

    @staticmethod
    def modifier(field, join_field):
        return ((field - join_field) * 100) / fn.NullIf(join_field, 0)

    @staticmethod
    def generate_key(reference_key):
        return '%s_delta_percent' % reference_key


class DoD(Reference):
    key = 'dod'
    label = 'DoD'
    time_unit = 'day'
    interval = 1


class WoW(Reference):
    key = 'wow'
    label = 'WoW'
    time_unit = 'week'
    interval = 1


class MoM(Reference):
    key = 'mom'
    label = 'MoM'
    time_unit = 'month'
    interval = 1


class QoQ(Reference):
    key = 'qoq'
    label = 'QoQ'
    time_unit = 'quarter'
    interval = 1


class YoY(Reference):
    key = 'yoy'
    label = 'YoY'
    time_unit = 'year'
    interval = 1
