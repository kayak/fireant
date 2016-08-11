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


class Delta(object):
    class WoW(Reference):
        key = 'wow_d'
        label = 'ΔWoW'

    class MoM(Reference):
        key = 'mom_d'
        label = 'ΔMoM'

    class QoQ(Reference):
        key = 'qoq_d'
        label = 'ΔQoQ'

    class YoY(Reference):
        key = 'yoy_d'
        label = 'ΔYoY'


class DeltaPercentage(object):
    class WoW(Reference):
        key = 'wow_p'
        label = 'ΔWoW%'

    class MoM(Reference):
        key = 'mom_p'
        label = 'ΔMoM%'

    class QoQ(Reference):
        key = 'qoq_p'
        label = 'ΔQoQ%'

    class YoY(Reference):
        key = 'yoy_p'
        label = 'ΔYoY%'
