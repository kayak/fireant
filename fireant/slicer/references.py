# coding: utf-8

class Reference(object):
    def __init__(self, element_key):
        self.element_key = element_key


class WoW(Reference):
    key = 'wow'


class MoM(Reference):
    key = 'mom'


class QoQ(Reference):
    key = 'qoq'


class YoY(Reference):
    key = 'yoy'


class Delta(object):
    class WoW(Reference):
        key = 'wow_d'

    class MoM(Reference):
        key = 'mom_d'

    class QoQ(Reference):
        key = 'qoq_d'

    class YoY(Reference):
        key = 'yoy_d'


class DeltaPercentage(object):
    class WoW(Reference):
        key = 'wow_p'

    class MoM(Reference):
        key = 'mom_p'

    class QoQ(Reference):
        key = 'qoq_p'

    class YoY(Reference):
        key = 'yoy_p'
