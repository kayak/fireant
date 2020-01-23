import itertools

from fireant.utils import (
    immutable,
    deepcopy,
    ordered_distinct_list,
    ordered_distinct_list_by_attr,
)
from fireant.queries.builder import (
    DataSetQueryBuilder,
    DimensionChoicesQueryBuilder,
    DimensionLatestQueryBuilder,
)


class _Container(object):
    """
    This is a list of dataset elements, metrics or dimensions, used for accessing an element by key with a dot syntax.

    Example:

    .. code-block:: python

        dataset = DataSet(
            dimensions=[
                Dimension(key='my_dimension1')
            ]
        )
        dataset.dimensions.my_dimension1
    """

    def __init__(self, items=()):
        self._items = ordered_distinct_list_by_attr(items)
        for item in self._items:
            setattr(self, item.alias, item)

    def __deepcopy__(self, memodict={}):
        for field in self:
            memodict[id(field)] = field
        return deepcopy(self, memodict)

    def __iter__(self):
        return iter(self._items)

    def __getitem__(self, item):
        return getattr(self, item)

    def __contains__(self, item):
        from .fields import Field

        if isinstance(item, str):
            return hasattr(self, item)

        if not isinstance(item, Field):
            return False

        self_item = getattr(self, item.alias, None)
        return item is self_item

    def __hash__(self):
        return hash((item for item in self._items))

    def __eq__(self, other):
        """
        Checks if the other object is an instance of _Container and has the same number of items with matching keys.
        """
        return isinstance(other, _Container) and all(
            [
                a is not None and b is not None and a.alias == b.alias
                for a, b in itertools.zip_longest(
                    self._items, getattr(other, "_items", ())
                )
            ]
        )

    def append(self, item):
        self._items.append(item)
        setattr(self, item.alias, item)


class DataSet:
    """
    The DataSet class abstracts the query generation, given the fields and what not that were provided, and the
    fetching of the aforementioned query's data.
    """

    class Fields(_Container):
        pass

    def __init__(
        self, table, database, joins=(), fields=(), always_query_all_metrics=False
    ):
        """
        Constructor for a dataset.  Contains all the fields to initialize the dataset.

        :param table: (Required)
            A pypika Table reference. The primary table that this dataset will retrieve data from.

        :param database:  (Required)
            A Database reference. Holds the connection details used by this dataset to execute queries.

        :param fields: (Required: At least one)
            A list of fields mapping definitions of data in the data set. Fields are similar to a column in a database
            query result set. They are the values

        :param joins:  (Optional)
            A list of join descriptions for joining additional tables.  Joined tables are only used when querying a
            metric or dimension which requires it.

        :param always_query_all_metrics: (Default: False)
            When true, all metrics will be included in database queries in order to increase cache hits.
        """
        self.table = table
        self.database = database
        self.joins = list(joins)

        self.fields = DataSet.Fields(fields)

        # add query builder entry points
        self.query = DataSetQueryBuilder(self)
        self.latest = DimensionLatestQueryBuilder(self)
        self.always_query_all_metrics = always_query_all_metrics

        for field in fields:
            if not field.definition.is_aggregate:
                field.choices = DimensionChoicesQueryBuilder(self, field)

    def __eq__(self, other):
        return isinstance(other, DataSet) and self.fields == other.fields

    def __repr__(self):
        return "DataSet(fields=[{}])".format(",".join([repr(f) for f in self.fields]))

    def __hash__(self):
        return hash(
            (
                self.table,
                self.database.database,
                tuple(self.joins),
                self.fields,
                self.always_query_all_metrics,
            )
        )

    @immutable
    def extra_fields(self, *fields):
        for field in fields:
            self.fields.append(field)

    def blend(self, other):
        """
        Returns a Data Set blender which enables to execute queries on multiple data sets and combine them.
        """
        from .data_blending import DataSetBlenderBuilder

        return DataSetBlenderBuilder(self, other)
