# coding: utf-8

class Reference(object):
    def __init__(self, element_key):
        self.element_key = element_key


class WoW(Reference):
    key = 'wow'
    label = 'WoW'


class MoM(Reference):
    key = 'mom'
    label = 'MoM'


class QoQ(Reference):
    key = 'qoq'
    label = 'QoQ'


class YoY(Reference):
    key = 'yoy'
    label = 'YoY'


class Delta(Reference):
    def __init__(self, reference):
        super(Delta, self).__init__(reference.element_key)
        self.key = '%s_d' % reference.key
        self.label = '%s Δ' % reference.label


class DeltaPercentage(Reference):
    def __init__(self, reference):
        super(DeltaPercentage, self).__init__(reference.element_key)
        self.key = '%s_p' % reference.key
        self.label = '%s Δ%%' % reference.label
