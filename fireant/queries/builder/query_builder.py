from typing import TYPE_CHECKING, Union

from pypika import Order

from fireant.dataset.fields import Field
from fireant.exceptions import DataSetException
from fireant.utils import (
    deepcopy,
    immutable,
)
from ..execution import fetch_data
from ..finders import find_field_in_modified_field
from ..sets import (
    apply_set_dimensions,
    omit_set_filters,
)

if TYPE_CHECKING:
    from fireant.dataset import DataSet


class QueryException(DataSetException):
    pass


def add_hints(queries, hint=None):
    return [
        query.hint(hint)
        if hint is not None and hasattr(query.__class__, "hint")
        else query
        for query in queries
    ]


def get_column_names(database, table):
    column_definitions = database.get_column_definitions(
        table._schema._name, table._table_name
    )

    return {column_definition[0] for column_definition in column_definitions}


def validate_fields(fields, dataset):
    fields = [find_field_in_modified_field(field) for field in fields]

    invalid = [field.alias for field in fields if field not in dataset.fields]
    if not invalid:
        return

    raise DataSetException(
          "Only fields from dataset can be used in a dataset query. Found invalid fields: {}.".format(
                ", ".join(invalid)
          )
    )


def _strip_modifiers(fields):
    for field in fields:
        node = field
        while hasattr(node, "dimension"):
            node = node.dimension
        yield node


class QueryBuilder(object):
    """
    This is the base class for building dataset queries. This class provides an interface for building dataset queries
    via a set of functions which can be chained together.
    """

    def __init__(self, dataset: 'DataSet'):
        """
        :param dataset: DataSet to build the query for
        """
        self.dataset = dataset
        self.table = dataset.table
        self._dimensions = []
        self._filters = []
        self._orders = None
        self._client_limit = None
        self._client_offset = None
        self._query_limit = None
        self._query_offset = None

    def __deepcopy__(self, memodict={}):
        fields = [d for d in self._dimensions]
        if self._orders is not None:
            fields += [field for (field, _) in self._orders]

        for field in fields:
            field = find_field_in_modified_field(field)
            memodict[id(field)] = field

        return deepcopy(self, memodict)

    @immutable
    def dimension(self, *dimensions):
        """
        Add one or more dimensions when building a dataset query.

        :param dimensions:
            Dimensions to add to the query
        :return:
            A copy of the query with the dimensions added.
        """
        validate_fields(dimensions, self.dataset)
        aliases = {dimension.alias for dimension in self._dimensions}
        self._dimensions += [
            dimension for dimension in dimensions if dimension.alias not in aliases
        ]

    @immutable
    def filter(self, *filters):
        """
        Add one or more filters when building a dataset query.

        :param filters:
            Filters to add to the query
        :return:
            A copy of the query with the filters added.
        """
        validate_fields([fltr.field for fltr in filters], self.dataset)
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
        validate_fields([field], self.dataset)

        if self._orders is None:
            self._orders = []

        if field is not None:
            self._orders += [(field, orientation)]

    @immutable
    def limit_query(self, limit):
        """
        Sets the limit of the query.

        :param limit:
            A limit on the number of database rows returned.
        :return:
            A copy of the query with the query limit set.
        """
        self._query_limit = limit

    @immutable
    def offset_query(self, offset):
        """
        Sets the offset of the query.

        :param offset:
            A offset on the number of database rows returned.
        :return:
            A copy of the query with the query offset set.
        """
        self._query_offset = offset

    @immutable
    def limit_client(self, limit):
        """
        Sets the limit to be used when paginating the Pandas Dataframe after the results
        have been returned

        :param limit:
            A limit on the number of dataframe rows returned.
        :return:
            A copy of the query with the dataframe limit set.
        """
        self._client_limit = limit

    @immutable
    def offset_client(self, offset):
        """
        Sets the offset to be used when paginating the Pandas Dataframe after the results
        have been returned

        :param offset:
            A offset on the number of dataframe rows returned
        :return:
            A copy of the query with the dataframe offset set.
        """
        self._client_offset = offset

    @property
    def dimensions(self):
        """
        Returns a list of Field instances, that might include newly created set dimensions.

        Set dimensions are generated at this level because the `ResultSet` filter modifier can artificially
        create dimensions, which widgets need to be aware in order to properly render the data. Moving this to
        `make_slicer_query_with_totals_and_references` function is not possible, even if we defaulted to using
        same alias as the referenced dimension/metric, given that would cause aliases clashes. Dimensions can
        be mostly replaced, but that's not the case for metrics.

        :return: A list of Field instances.
        """
        return apply_set_dimensions(self._dimensions, self._filters, self.dataset)

    @property
    def filters(self):
        """
        Returns a list of Filter instances, that might omit filters wrapped with `ResultSet` filter modifier.

        :return: A list of Filter instances.
        """
        return omit_set_filters(self._filters)

    @property
    def orders(self):
        """
        Return orders.

        :return: None or a list of tuples shaped as Field instance and ordering.
        """
        return self._orders

    @property
    def default_orders(self):
        """
        Return orders based on the provided dimensions.

        :return: A list of tuples shaped as Field instance and ordering.
        """
        dimension_orders = []

        for dimension in self.dimensions:
            if not dimension.is_aggregate:
                dimension_orders.append((dimension, None))

        return dimension_orders

    @property
    def sql(self):
        """
        Serialize this query builder object to a set of Pypika/SQL queries.

        This is the base implementation shared by two implementations: the query to fetch data for a dataset request and
        the query to fetch choices for dimensions.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The dataset query extends this with metrics, references, and totals.
        """
        raise NotImplementedError()

    def fetch(self, hint=None):
        """
        Fetches the data for this query instance and returns it in an instance of `pd.DataFrame`

        :param hint:
            For database vendors that support it, add a query hint to collect analytics on the queries triggered by
            fireant.
        """
        queries = add_hints(self.sql, hint)

        max_rows_returned, data = fetch_data(self.dataset.database, queries, self.dimensions)
        return self._transform_for_return(data, max_rows_returned=max_rows_returned)

    def _apply_pagination(self, query):
        query = query.limit(min(self._query_limit or float('inf'), self.dataset.database.max_result_set_size))
        return query.offset(self._query_offset)

    def _transform_for_return(self, widget_data, **metadata) -> Union[dict, list]:
        return dict(data=widget_data, metadata=dict(**metadata)) \
            if self.dataset.return_additional_metadata \
            else widget_data


class ReferenceQueryBuilderMixin:
    """
    This is a mixin class for building dataset queries that allow references. This class provides an interface for
    building dataset queries via a set of functions which can be chained together.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._references = []

    @immutable
    def reference(self, *references):
        """
        Add one or more references for a dimension when building a dataset query.

        :param references:
            References to add to the query
        :return:
            A copy of the query with the references added.
        """
        validate_fields([reference.field for reference in references], self.dataset)
        self._references += references


class WidgetQueryBuilderMixin:
    """
    This is a mixin class for building dataset queries that allow widgets. This class provides an interface for
    building dataset queries via a set of functions which can be chained together.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._widgets = []

    def _validate(self):
        for widget in self._widgets:
            if hasattr(widget, "validate"):
                widget.validate(self._dimensions)

    @immutable
    def widget(self, *widgets):
        """
        Add one or more widgets when building a dataset query.

        :param widgets:
            Widgets to add to the query
        :return:
            A copy of the query with the widgets added.
        """
        validate_fields(
              [field for widget in widgets for field in widget.metrics], self.dataset
        )

        self._widgets += widgets
