# coding: utf-8
from pypika import Interval, functions as fn


class Reference(object):
    interval = None
    modifier = None

    def __init__(self, element_key):
        self.element_key = element_key


class Delta(Reference):
    def __init__(self, reference):
        super(Delta, self).__init__(reference.element_key)
        self.key = '%s_d' % reference.key
        self.label = '%s Δ' % reference.label
        self.interval = reference.interval

    @staticmethod
    def modifier(field, join_field):
        return field - join_field


class DeltaPercentage(Reference):
    def __init__(self, reference):
        super(DeltaPercentage, self).__init__(reference.element_key)
        self.key = '%s_p' % reference.key
        self.label = '%s Δ%%' % reference.label
        self.interval = reference.interval

    @staticmethod
    def modifier(field, join_field):
        return (field - join_field) / fn.NullIf(join_field, 0)


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
