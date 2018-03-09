import itertools

from .dimensions import (
    CategoricalDimension,
    DisplayDimension,
    UniqueDimension,
)
from .queries import (
    DimensionOptionQueryBuilder,
    SlicerQueryBuilder,
)


class _Container(object):
    """
    This is a list of slicer elements, metrics or dimensions, used for accessing an element by key with a dot syntax.

    For Example:
    ```
    slicer = Slicer(
        dimensions=[
            Dimension(key='my_dimension1')
        ]
    )
    slicer.dimensions.my_dimension1
    ```
    """
    def __init__(self, items):
        self._items = items
        for item in items:
            setattr(self, item.key, item)

            # Special case to include display definitions for filters
            if item.has_display_field:
                setattr(self, item.display_key, DisplayDimension(item))

    def __iter__(self):
        return iter(self._items)

    def __getitem__(self, item):
        return getattr(self, item)

    def __eq__(self, other):
        """
        Checks if the other object is an instance of _Container and has the same number of items with matching keys.
        """
        return isinstance(other, _Container) \
               and all([a is not None
                        and b is not None
                        and a.key == b.key
                        for a, b in itertools.zip_longest(self._items, getattr(other, '_items', ()))])


class Slicer(object):
    """
    WRITEME
    """

    class Dimensions(_Container):
        pass

    class Metrics(_Container):
        pass

    def __init__(self, table, database, joins=(), dimensions=(), metrics=(), hint_table=None):
        """
        Constructor for a slicer.  Contains all the fields to initialize the slicer.

        :param table: (Required)
            A pypika Table reference. The primary table that this slicer will retrieve data from.

        :param database:  (Required)
            A Database reference. Holds the connection details used by this slicer to execute queries.

        :param metrics: (Required: At least one)
            A list of metrics which can be queried.  Metrics are the types of data that are displayed.

        :param dimensions: (Optional)
            A list of dimensions used for grouping metrics.  Dimensions are used as the axes in charts, the indices in
            tables, and also for splitting charts into multiple lines.

        :param joins:  (Optional)
            A list of join descriptions for joining additional tables.  Joined tables are only used when querying a
            metric or dimension which requires it.

        :param hint_table: (Optional)
            A hint table used for querying dimension options.  If not present, the table will be used.  The hint_table
            must have the same definition as the table omitting dimensions which do not have a set of options (such as
            datetime or boolean dimensions) and the metrics.  This is provided to more efficiently query dimension
            options.
        """
        self.table = table
        self.database = database
        self.joins = joins

        self.hint_table = hint_table
        self.dimensions = Slicer.Dimensions(dimensions)
        self.metrics = Slicer.Metrics(metrics)

        # add query builder entry points
        self.data = SlicerQueryBuilder(self)
        for dimension in dimensions:
            if not isinstance(dimension, (UniqueDimension, CategoricalDimension)):
                continue
            dimension.options = DimensionOptionQueryBuilder(self, dimension)

    def __eq__(self, other):
        return isinstance(other, Slicer) \
               and self.metrics == other.metrics \
               and self.dimensions == other.dimensions

    def __repr__(self):
        return 'Slicer(metrics=[{}],dimensions=[{}])' \
            .format(','.join([m.key for m in self.metrics]),
                    ','.join([d.key for d in self.dimensions]))
