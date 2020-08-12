import itertools

from fireant.queries.builder import (
    DataSetQueryBuilder,
    DimensionChoicesQueryBuilder,
    DimensionLatestQueryBuilder,
)
from fireant.utils import (
    deepcopy,
    immutable,
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
    def __new__(cls, *args, **kwargs):
        """
        Because we override __getattr__ we need to implement __new__ in order for pickling to work correctly:
        https://docs.python.org/3/library/pickle.html#object.__getstate__
        """
        new_instance = super().__new__(cls)
        new_instance._items = {}
        return new_instance

    def __init__(self, items=(), key_attribute="alias"):
        self._key_attribute = key_attribute
        for item in items:
            self.add(item)

    def __deepcopy__(self, memodict={}):
        for field in self:
            memodict[id(field)] = field
        return deepcopy(self, memodict)

    def __iter__(self):
        return iter(self._items.values())

    def __getitem__(self, item):
        return getattr(self, item)

    def __getattr__(self, key):
        result = self._items.get(key)
        if result is not None:
            return result
        raise AttributeError

    def __contains__(self, item):
        from .fields import Field

        if isinstance(item, str):
            return item in self._items

        if not isinstance(item, Field):
            return False

        key = getattr(item, self._key_attribute)
        self_item = self._items.get(key)
        return item is self_item

    def __hash__(self):
        return hash((item for item in self._items.values()))

    def __eq__(self, other):
        """
        Checks if the other object is an instance of _Container and has the same number of items with matching keys.
        """
        return isinstance(other, _Container) and all(
            [
                a is not None and b is not None and a.alias == b.alias
                for a, b in itertools.zip_longest(
                    self._items.values(), getattr(other, "_items", {}).values()
                )
            ]
        )

    def add(self, item):
        key = getattr(item, self._key_attribute)
        if key in self._items:
            raise ValueError(f"Item with key {key} already exists.")

        # A very effective way to not allow reserved words but maybe not too great for performance
        if key in dir(self):
            raise ValueError(f"Reserved name {key} can not be used.")

        self._items[key] = item


class DataSet:
    """
    The DataSet class abstracts the query generation, given the fields and what not that were provided, and the
    fetching of the aforementioned query's data.
    """

    class Fields(_Container):
        pass

    def __init__(
        self,
        table,
        database,
        joins=(),
        annotation=None,
        fields=(),
        always_query_all_metrics: bool = False,
        return_additional_metadata: bool = False,
    ):
        """
        Constructor for a dataset.  Contains all the fields to initialize the dataset.

        :param table: (Required)
            A pypika Table reference. The primary table that this dataset will retrieve data from.
        :param database:  (Required)
            A Database reference. Holds the connection details used by this dataset to execute queries.
        :param fields: (Required: At least one)
            A list of fields mapping definitions of data in the data set. Fields are similar to a column in a database
            query result set. They are the values.
        :param joins:  (Optional)
            A list of join descriptions for joining additional tables.  Joined tables are only used when querying a
            metric or dimension which requires it.
        :param annotation:  (Optional)
            Annotation for fetching additional data for a dataset.
        :param always_query_all_metrics: (Default: False)
            When true, all metrics will be included in database queries in order to increase cache hits.
        :param return_additional_metadata: (Default: False)
            When true, widget data will be enveloped so extra metadata can be added to the response
            as follows: {'data': <widget data>, 'metadata': {...}}
        """
        self.table = table
        self.database = database
        self.joins = list(joins)
        self.annotation = annotation

        self.fields = DataSet.Fields(fields)

        # add query builder entry points
        self.query = DataSetQueryBuilder(self)
        self.latest = DimensionLatestQueryBuilder(self)
        self.always_query_all_metrics = always_query_all_metrics
        self.return_additional_metadata = return_additional_metadata

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
            self.fields.add(field)

    def blend(self, other):
        """
        Returns a Data Set blender which enables to execute queries on multiple data sets and combine them.
        """
        from .data_blending import DataSetBlenderBuilder

        return DataSetBlenderBuilder(self, other)
