from pypika import Order

from fireant.dataset.fields import Field
from fireant.exceptions import SlicerException
from fireant.utils import (
    alias_selector,
    immutable,
)
from ..execution import fetch_data


class QueryException(SlicerException):
    pass


def add_hints(queries, hint=None):
    return [query.hint(hint)
            if hint is not None and hasattr(query.__class__, 'hint')
            else query
            for query in queries]


def get_column_names(database, table):
    column_definitions = database.get_column_definitions(
          table._schema._name,
          table._table_name
    )

    return {column_definition[0] for column_definition in column_definitions}


class QueryBuilder(object):
    """
    This is the base class for building slicer queries. This class provides an interface for building slicer queries
    via a set of functions which can be chained together.
    """

    def __init__(self, dataset, table):
        self.dataset = dataset
        self.table = table
        self._dimensions = []
        self._filters = []
        self._orders = []
        self._limit = None
        self._offset = None

    @immutable
    def dimension(self, *dimensions):
        """
        Add one or more dimensions when building a slicer query.

        :param dimensions:
            Dimensions to add to the query
        :return:
            A copy of the query with the dimensions added.
        """
        aliases = {dimension.alias
                   for dimension in self._dimensions}
        self._dimensions += [dimension
                             for dimension in dimensions
                             if dimension.alias not in aliases]

    @immutable
    def filter(self, *filters):
        """
        Add one or more filters when building a slicer query.

        :param filters:
            Filters to add to the query
        :return:
            A copy of the query with the filters added.
        """
        self._filters += [f for f in filters]

    @immutable
    def orderby(self, field: Field, orientation: Order = None):
        """
        :param field:
            The element to order by, either a metric or dimension.
        :param orientation:
            The directionality to order by, either ascending or descending.
        :return:
            A copy of the query with the order by added.
        """
        self._orders += [(field.definition.as_(alias_selector(field.alias)), orientation)]

    @immutable
    def limit(self, limit):
        """
        Sets the limit of the query.

        :param limit:
            A limit on the number of database rows returned.
        :return:
            A copy of the query with the limit set.
        """
        self._limit = limit

    @immutable
    def offset(self, offset):
        """
        Sets the offset of the query.

        :param offset:
            A offset on the number of database rows returned.
        :return:
            A copy of the query with the ofset set.
        """
        self._offset = offset

    @property
    def sql(self):
        """
        Serialize this query builder object to a set of Pypika/SQL queries.

        This is the base implementation shared by two implementations: the query to fetch data for a slicer request and
        the query to fetch choices for dimensions.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.
        """
        raise NotImplementedError()

    def fetch(self, hint=None):
        """
        Fetches the data for this query instance and returns it in an instance of `pd.DataFrame`

        :param hint:
            For database vendors that support it, add a query hint to collect analytics on the queries triggerd by
            fireant.
        """
        queries = add_hints(self.sql, hint)

        return fetch_data(self.dataset.database, queries, self._dimensions)


class ReferenceQueryBuilderMixin(object):
    """
    This is a mixin class for building slicer queries that allow references. This class provides an interface for
    building slicer queries via a set of functions which can be chained together.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self._references = []

    @immutable
    def reference(self, *references):
        """
        Add one or more references for a dimension when building a slicer query.

        :param references:
            References to add to the query
        :return:
            A copy of the query with the references added.
        """
        self._references += references


class WidgetQueryBuilderMixin(object):
    """
    This is a mixin class for building slicer queries that allow widgets. This class provides an interface for
    building slicer queries via a set of functions which can be chained together.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self._widgets = []

    def _validate(self):
        for widget in self._widgets:
            if hasattr(widget, 'validate'):
                widget.validate(self._dimensions)

    @immutable
    def widget(self, *widgets):
        """
        Add one or more widgets when building a slicer query.

        :param widgets:
            Widgets to add to the query
        :return:
            A copy of the query with the widgets added.
        """
        self._widgets += widgets
