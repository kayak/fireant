from .queries import QueryBuilder


class _Container(object):
    def __init__(self, items):
        self._items = items
        for item in items:
            setattr(self, item.key, item)

    def __iter__(self):
        return iter(self._items)


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
            A Pypika Table reference. The primary table that this slicer will retrieve data from.

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
            datetime dimensions) and the metrics.  This is provided to more efficiently query dimension options.
        """
        self.table = table
        self.database = database
        self.joins = joins
        self.dimensions = Slicer.Dimensions(dimensions)
        self.metrics = Slicer.Metrics(metrics)
        self.hint_table = hint_table

    def query(self):
        """
        WRITEME
        """
        return QueryBuilder(self)
