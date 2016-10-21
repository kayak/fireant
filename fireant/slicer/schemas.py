# coding: utf-8
from fireant.slicer import transformers
from fireant.slicer.managers import SlicerManager, TransformerManager
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

    def schemas(self, *args, **kwargs):
        return [
            (self.key, self.definition)
        ]


class Metric(SlicerElement):
    """
    The `Metric` class represents a metric in the `Slicer` object.
    """

    def __init__(self, key, label=None, definition=None, joins=None, precision=None, prefix=None, suffix=None):
        super(Metric, self).__init__(key, label, definition, joins)
        self.precision = precision
        self.prefix = prefix
        self.suffix = suffix


class Dimension(SlicerElement):
    """
    The `Dimension` class represents a dimension in the `Slicer` object.
    """

    def __init__(self, key, label=None, definition=None, joins=None):
        super(Dimension, self).__init__(key, label, definition, joins)

    def levels(self):
        return [self.key]


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

    def schemas(self, *args, **kwargs):
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

    def schemas(self, *args, database=None, **kwargs):
        interval = args[0] if args else self.default_interval
        return [(self.key, database.round_date(self.definition, interval.size))]


class CategoricalDimension(Dimension):
    def __init__(self, key, label=None, definition=None, display_options=tuple(), joins=None):
        super(CategoricalDimension, self).__init__(key=key, label=label, definition=definition, joins=joins)
        self.display_options = display_options


class UniqueDimension(Dimension):
    def __init__(self, key, label=None, definition=None, display_field=None, joins=None):
        super(UniqueDimension, self).__init__(key=key, label=label, definition=definition, joins=joins)
        self.display_field = display_field

    def schemas(self, *args, **kwargs):
        id_field_schemas = [('{key}'.format(key=self.key), self.definition)]
        return id_field_schemas + [('{key}_display'.format(key=self.key), self.display_field)]

    def levels(self):
        if self.display_field is not None:
            return [self.key, self.display_field]
        return super(UniqueDimension, self).levels()


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
    def __init__(self, key, table, criterion, join_type=JoinType.inner):
        self.key = key
        self.table = table
        self.criterion = criterion
        self.join_type = join_type


class Slicer(object):
    def __init__(self, table, database, metrics=tuple(), dimensions=tuple(), joins=tuple()):
        self.table = table
        self.database = database

        self.metrics = {metric.key: metric for metric in metrics}
        self.dimensions = {dimension.key: dimension for dimension in dimensions}
        self.joins = {join.key: join for join in joins}

        self.manager = SlicerManager(self)
        for name, bundle in transformers.bundles.items():
            setattr(self, name, TransformerManager(self.manager, bundle))


class EqualityOperator(object):
    eq = 'eq'
    ne = 'ne'
    gt = 'gt'
    lt = 'lt'
    gte = 'gte'
    lte = 'lte'
