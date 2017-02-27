# coding: utf-8
from pypika import Interval, functions as fn


class Reference(object):
    key = None
    label = None
    interval = None
    modifier = None

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
    interval = Interval(days=1)


class WoW(Reference):
    key = 'wow'
    label = 'WoW'
    interval = Interval(weeks=1)


class MoM(Reference):
    key = 'mom'
    label = 'MoM'
    interval = Interval(months=1)


class QoQ(Reference):
    key = 'qoq'
    label = 'QoQ'
    interval = Interval(quarters=1)


class YoY(Reference):
    key = 'yoy'
    label = 'YoY'
    interval = Interval(years=1)
