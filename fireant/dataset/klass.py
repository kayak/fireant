import itertools

from fireant.queries import (
    DataSetQueryBuilder,
    DimensionChoicesQueryBuilder,
    DimensionLatestQueryBuilder,
)


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
    def __init__(self, items):
        self._items = items
        for item in items:
            setattr(self, item.alias, item)

    def __iter__(self):
        return iter(self._items)

    def __getitem__(self, item):
        return getattr(self, item)

    def __contains__(self, item):
        return hasattr(self, item)

    def __eq__(self, other):
        """
        Checks if the other object is an instance of _Container and has the same number of items with matching keys.
        """
        return isinstance(other, _Container) and \
               all([a is not None
                    and b is not None
                    and a.alias == b.alias
                    for a, b in itertools.zip_longest(self._items, getattr(other, '_items', ()))])


class DataSet(object):
    """
    WRITEME
    """

    class Fields(_Container):
        pass

    def __init__(self, table, database, joins=(), fields=(),
                 hint_table=None,
                 always_query_all_metrics=False):
        """
        Constructor for a slicer.  Contains all the fields to initialize the slicer.

        :param table: (Required)
            A pypika Table reference. The primary table that this slicer will retrieve data from.

        :param database:  (Required)
            A Database reference. Holds the connection details used by this slicer to execute queries.

        :param fields: (Required: At least one)
            A list of fields mapping definitions of data in the data set. Fields are similar to a column in a database
            query result set. They are the values

        :param joins:  (Optional)
            A list of join descriptions for joining additional tables.  Joined tables are only used when querying a
            metric or dimension which requires it.

        :param hint_table: (Optional)
            A hint table used for querying dimension options.  If not present, the table will be used.  The hint_table
            must have the same definition as the table omitting dimensions which do not have a set of options (such as
            datetime or boolean dimensions) and the metrics.  This is provided to more efficiently query dimension
            options.

        :param always_query_all_metrics: (Default: False)
            When true, all metrics will be included in database queries in order to increase cache hits.
        """
        self.table = table
        self.database = database
        self.joins = joins

        self.hint_table = hint_table
        self.fields = DataSet.Fields(fields)

        # add query builder entry points
        self.query = DataSetQueryBuilder(self)
        self.latest = DimensionLatestQueryBuilder(self)

        self.always_query_all_metrics = always_query_all_metrics

        for field in fields:
            if not field.definition.is_aggregate:
                field.choices = DimensionChoicesQueryBuilder(self, field)

    def __eq__(self, other):
        return isinstance(other, DataSet) \
               and self.fields == other.fields

    def __repr__(self):
        return 'Slicer(fields=[{}])' \
            .format(','.join([repr(f)
                              for f in self.fields]))
