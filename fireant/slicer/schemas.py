# coding: utf-8
from fireant import settings
from fireant.slicer.managers import SlicerManager
from pypika import JoinType
from pypika.terms import Mod


class SlicerElement(object):
    """The `SlicerElement` class represents an element of the slicer, either a metric or dimension, which contains
    information about such as how to query it from the database."""

    def __init__(self, key, label=None, definition=None, joins=None):
        """
        :param key:
            The unique identifier of the slicer element, used in the Slicer manager API to reference a defined element.

        :param label:
            A displayable representation of the column.  Defaults to the key capitalized.

        :param definition:
            The definition of the element as a PyPika expression which defines how to query it from the database.

        :param joins:
            A list of Join keys required by this element.
        """
        self.key = key
        self.label = label or ' '.join(key.capitalize().split('_'))
        self.definition = definition
        self.joins = joins

    def __unicode__(self):
        return self.key

    def __repr__(self):
        return self.key

    def schemas(self, *args):
        return [
            (self.key, self.definition)
        ]


class Metric(SlicerElement):
    """
    The `Metric` class represents a metric in the `Slicer` object.
    """

    def __init__(self, key, label=None, definition=None, joins=None):
        super(Metric, self).__init__(key, label, definition, joins)


class Dimension(SlicerElement):
    """
    The `Dimension` class represents a dimension in the `Slicer` object.
    """

    def __init__(self, key, label=None, definition=None, joins=None):
        super(Dimension, self).__init__(key, label, definition, joins)


class NumericInterval(object):
    def __init__(self, size=1, offset=0):
        self.size = size
        self.offset = offset

    def __eq__(self, other):
        return isinstance(other, NumericInterval) and self.size == other.size and self.offset == other.offset

    def __hash__(self):
        return hash('NumericInterval %d %d' % (self.size, self.offset))

    def __str__(self):
        return 'NumericInterval(size=%d,offset=%d)' % (self.size, self.offset)


class ContinuousDimension(Dimension):
    def __init__(self, key, label=None, definition=None, default_interval=NumericInterval(1, 0), joins=None):
        super(ContinuousDimension, self).__init__(key=key, label=label, definition=definition, joins=joins)
        self.default_interval = default_interval

    def schemas(self, *args):
        size, offset = args if args else (self.default_interval.size, self.default_interval.offset)
        return [(self.key, Mod(self.definition + offset, size))]


class DatetimeInterval(object):
    def __init__(self, size):
        self.size = size

    def __eq__(self, other):
        return isinstance(other, DatetimeInterval) and self.size == other.size

    def __hash__(self):
        return hash('DatetimeInterval %s' % self.size)

    def __str__(self):
        return 'DatetimeInterval(interval=%s)' % self.size


class DatetimeDimension(ContinuousDimension):
    hour = DatetimeInterval('HH')
    day = DatetimeInterval('DD')
    week = DatetimeInterval('WW')
    month = DatetimeInterval('MM')
    quarter = DatetimeInterval('Q')
    year = DatetimeInterval('IY')

    def __init__(self, key, label=None, definition=None, default_interval=day, joins=None):
        super(DatetimeDimension, self).__init__(key=key, label=label, definition=definition, joins=joins,
                                                default_interval=default_interval)

    def schemas(self, *args):
        interval = args[0] if args else self.default_interval
        return [(self.key, settings.database.round_date(self.definition, interval.size))]


class CategoricalDimension(Dimension):
    def __init__(self, key, label=None, definition=None, options=tuple(), joins=None):
        super(CategoricalDimension, self).__init__(key=key, label=label, definition=definition, joins=joins)
        self.options = options


class UniqueDimension(Dimension):
    def __init__(self, key, label=None, label_field=None, id_fields=None, joins=None):
        super(UniqueDimension, self).__init__(key=key, label=label, definition=id_fields, joins=joins)
        # TODO label_field and definition here are redundant
        self.label_field = label_field
        self.id_fields = id_fields

    def schemas(self, *args):
        id_field_schemas = [('{key}_id{ordinal}'.format(key=self.key, ordinal=i), id_field)
                            for i, id_field in enumerate(self.id_fields)]
        return id_field_schemas + [('{key}_label'.format(key=self.key), self.label_field)]


class BooleanDimension(Dimension):
    def __init__(self, key, label=None, definition=None, joins=None):
        super(BooleanDimension, self).__init__(key=key, label=label, definition=definition, joins=joins)


class DimensionValue(object):
    """
    An option belongs to a categorical dimension which specifies a fixed set of values
    """

    def __init__(self, key, label=None):
        self.key = key
        self.label = label or ' '.join(key.capitalize().split('_'))


class Join(object):
    def __init__(self, key, table, criterion, join_type=JoinType.left):
        self.key = key
        self.table = table
        self.criterion = criterion
        self.join_type = join_type


class Slicer(object):
    def __init__(self, table, metrics=tuple(), dimensions=tuple(), joins=tuple()):
        self.table = table

        self.metrics = {metric.key: metric for metric in metrics}
        self.dimensions = {dimension.key: dimension for dimension in dimensions}
        self.joins = {join.key: join for join in joins}

        self.manager = SlicerManager(self)


class EqualityOperator(object):
    eq = 'eq'
    ne = 'ne'
    gt = 'gt'
    lt = 'lt'
    gte = 'gte'
    lte = 'lte'
