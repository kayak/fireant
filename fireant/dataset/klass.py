import itertools

from fireant import (
    Field,
    Join,
)
from fireant.queries.builder import (
    DataSetQueryBuilder,
    DimensionChoicesQueryBuilder,
    DimensionLatestQueryBuilder,
)
from fireant.utils import immutable


class _Container(object):
    """
    This is a list of slicer elements, metrics or dimensions, used for accessing an element by key with a dot syntax.

    Example:

    .. code-block:: python

        slicer = Slicer(
            dimensions=[
                Dimension(key='my_dimension1')
            ]
        )
        slicer.dimensions.my_dimension1
    """

    def __init__(self):
        self._items = []

    def __iter__(self):
        return iter(self._items)

    def __getitem__(self, item):
        return getattr(self, item)

    def __contains__(self, item):
        return hasattr(self, item)

    def __hash__(self):
        return hash((item for item in self._items))

    def __eq__(self, other):
        """
        Checks if the other object is an instance of _Container and has the same number of items with matching keys.
        """
        return isinstance(other, _Container) and \
               all([a is not None
                    and b is not None
                    and a.alias == b.alias
                    for a, b in itertools.zip_longest(self._items, getattr(other, '_items', ()))])

    def append(self, item):
        self._items.append(item)
        setattr(self, item.alias, item)


class DataSet(object):
    """
    The DataSet class abstracts the query generation, given the fields and what not that were provided, and the
    fetching of the aforementioned query's data.
    """

    class Fields(_Container):
        pass

    def __init__(self, table, database, always_query_all_metrics=False):
        """
        Constructor for a slicer.  Contains all the fields to initialize the slicer.

        :param table: (Required)
            A pypika Table reference. The primary table that this slicer will retrieve data from.

        :param database:  (Required)
            A Database reference. Holds the connection details used by this slicer to execute queries.

        :param always_query_all_metrics: (Default: False)
            When true, all metrics will be included in database queries in order to increase cache hits.
        """
        self.table = table
        self.database = database
        self.joins = []

        self.fields = DataSet.Fields()

        # add query builder entry points
        self.query = DataSetQueryBuilder(self)
        self.latest = DimensionLatestQueryBuilder(self)
        self.always_query_all_metrics = always_query_all_metrics

    def __eq__(self, other):
        return isinstance(other, DataSet) \
               and self.fields == other.fields

    def __repr__(self):
        return 'Slicer(fields=[{}])' \
            .format(','.join([repr(f)
                              for f in self.fields]))

    def __hash__(self):
        return hash((self.table, self.database.database, tuple(self.joins), self.fields, self.always_query_all_metrics))

    @immutable
    def join(self, *args, **kwargs):
        """
        Adds a join when building a slicer query.

        :return:
            A copy of this DataSet instance with the join added.
        """

        self.joins.append(
            Join(*args, **kwargs)
        )

    @immutable
    def field(self, *args, field_class=Field, **kwargs):
        """
        Adds a field when building a slicer query. Fields are similar to a column in a database query result set.

        :param field_class: (Optional)
            A class that inherits from Field. That will be used for instantiating a new field with the provided
            args and kwargs. Defaults to Field.
        :return:
            A copy of this DataSet instance with the field added.
        """
        field = field_class(self, *args, **kwargs)

        if not field.definition.is_aggregate:
            field.choices = DimensionChoicesQueryBuilder(self, field)

        self.fields.append(
            field
        )
