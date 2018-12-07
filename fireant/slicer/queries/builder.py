from typing import (
    Dict,
    Iterable,
)

import pandas as pd

from fireant.utils import (
    format_dimension_key,
    format_metric_key,
    immutable,
    repr_field_key,
)
from pypika import Order
from . import special_cases
from .database import fetch_data
from .finders import (
    find_and_group_references_for_dimensions,
    find_and_replace_reference_dimensions,
    find_metrics_for_widgets,
    find_operations_for_widgets,
)
from .makers import (
    make_latest_query,
    make_orders_for_dimensions,
    make_slicer_query,
    make_slicer_query_with_rollup_and_references,
)
from .pagination import paginate
from .. import QueryException
from ..base import SlicerElement
from ..dimensions import Dimension
from ..references import reference_key


def add_hints(queries, hint=None):
    return [query.hint(hint)
            if hint is not None and hasattr(query.__class__, 'hint')
            else query
            for query in queries]


class QueryBuilder(object):
    """
    This is the base class for building slicer queries. This class provides an interface for building slicer queries
    via a set of functions which can be chained together.
    """

    def __init__(self, slicer, table):
        self.slicer = slicer
        self.table = table
        self._dimensions = []
        self._filters = []
        self._references = []
        self._limit = None
        self._offset = None

    @immutable
    def filter(self, *filters):
        """
        :param filters:
        :return:
        """
        self._filters += filters

    @immutable
    def limit(self, limit):
        """
        :param limit:
            A limit on the number of database rows returned.
        """
        self._limit = limit

    @immutable
    def offset(self, offset):
        """
        :param offset:
            A offset on the number of database rows returned.
        """
        self._offset = offset

    @property
    def queries(self):
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
        queries = add_hints(self.queries, hint)

        return fetch_data(self.slicer.database, queries, self._dimensions)


class SlicerQueryBuilder(QueryBuilder):
    """
    Slicer queries consist of widgets, dimensions, filters, and references. At least one or more widgets is required.
    All others are optional.
    """

    def __init__(self, slicer):
        super(SlicerQueryBuilder, self).__init__(slicer, slicer.table)
        self._widgets = []
        self._orders = []

    @immutable
    def widget(self, *widgets):
        """

        :param widgets:
        :return:
        """
        self._widgets += widgets

    @immutable
    def dimension(self, *dimensions):
        """

        :param dimensions:
        :return:
        """
        self._dimensions += [dimension
                             for dimension in dimensions
                             if dimension not in self._dimensions]

    @immutable
    def reference(self, *references):
        """
        Add a reference for a dimension when building a slicer query.

        :param references:
            References to add to the query
        :return:
            A copy of the dimension with the reference added.
        """
        self._references += references

    @immutable
    def orderby(self, element: SlicerElement, orientation: Order = None):
        """
        :param element:
            The element to order by, either a metric or dimension.
        :param orientation:
            The directionality to order by, either ascending or descending.
        :return:
        """
        format_key = format_dimension_key \
            if isinstance(element, Dimension) \
            else format_metric_key

        self._orders += [(element.definition.as_(format_key(element.key)), orientation)]

    @property
    def queries(self):
        """
        Serialize this query builder to a list of Pypika/SQL queries. This function will return one query for every
        combination of reference and rolled up dimension (including null options).

        This collects all of the metrics in each widget, dimensions, and filters and builds a corresponding pypika query
        to fetch the data.  When references are used, the base query normally produced is wrapped in an outer query and
        a query for each reference is joined based on the referenced dimension shifted.
        """
        # First run validation for the query on all widgets
        for widget in self._widgets:
            if hasattr(widget, 'validate'):
                widget.validate(self._dimensions)

        # Optionally select all metrics for slicer to better utilize caching
        metrics = list(self.slicer.metrics) \
            if self.slicer.always_query_all_metrics \
            else find_metrics_for_widgets(self._widgets)
        operations = find_operations_for_widgets(self._widgets)
        references = find_and_replace_reference_dimensions(self._references, self._dimensions)
        orders = (self._orders or make_orders_for_dimensions(self._dimensions))

        return make_slicer_query_with_rollup_and_references(self.slicer.database,
                                                            self.table,
                                                            self.slicer.joins,
                                                            self._dimensions,
                                                            metrics,
                                                            operations,
                                                            self._filters,
                                                            references,
                                                            orders)

    def fetch(self, hint=None) -> Iterable[Dict]:
        """
        Fetch the data for this query and transform it into the widgets.

        :param hint:
            A query hint label used with database vendors which support it. Adds a label comment to the query.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        queries = add_hints(self.queries, hint)

        reference_groups = list(find_and_group_references_for_dimensions(self._references).values())
        data_frame = fetch_data(self.slicer.database, queries, self._dimensions, reference_groups)

        # Apply operations
        operations = find_operations_for_widgets(self._widgets)
        for operation in operations:
            for reference in [None] + self._references:
                df_key = format_metric_key(reference_key(operation, reference))
                data_frame[df_key] = operation.apply(data_frame, reference)

        data_frame = special_cases.apply_operations_to_data_frame(operations, data_frame)

        data_frame = paginate(data_frame,
                              self._widgets,
                              orders=self._orders,
                              limit=self._limit, offset=self._offset)

        # Apply transformations
        return [widget.transform(data_frame, self.slicer, self._dimensions, self._references)
                for widget in self._widgets]

    def __str__(self):
        return str(self.queries)

    def __repr__(self):
        return ".".join(["slicer", "data"]
                        + ["widget({})".format(repr(widget))
                           for widget in self._widgets]
                        + ["dimension({})".format(repr(dimension))
                           for dimension in self._dimensions]
                        + ["filter({})".format(repr(filter))
                           for filter in self._filters]
                        + ["reference({})".format(repr(reference))
                           for reference in self._references]
                        + ["orderby({}, {})".format(repr_field_key(definition.alias),
                                                    orientation)
                           for (definition, orientation) in self._orders])


class DimensionChoicesQueryBuilder(QueryBuilder):
    """
    This builder is used for building slicer queries for fetching the choices for a dimension given a set of filters.
    """

    def __init__(self, slicer, dimension):
        super(DimensionChoicesQueryBuilder, self).__init__(slicer, slicer.hint_table or slicer.table)
        self._dimensions.append(dimension)

    @property
    def queries(self):
        """
        Serializes this query builder as a set of SQL queries.  This method will always return a list of one query since
        only one query is required to retrieve dimension choices.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.
        """
        query = make_slicer_query(database=self.slicer.database,
                                  base_table=self.table,
                                  joins=self.slicer.joins,
                                  dimensions=self._dimensions,
                                  filters=self._filters) \
            .limit(self._limit) \
            .offset(self._offset)

        return [query]

    def fetch(self, hint=None, force_include=()) -> pd.Series:
        """
        Fetch the data for this query and transform it into the widgets.

        :param hint:
            For database vendors that support it, add a query hint to collect analytics on the queries triggerd by
            fireant.
        :param force_include:
            A list of dimension values to include in the result set. This can be used to avoid having necessary results
            cut off due to the pagination.  These results will be returned at the head of the results.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        query = add_hints(self.queries, hint)[0]

        dimension = self._dimensions[0]
        definition = dimension.display_definition.as_(format_dimension_key(dimension.display_key)) \
            if dimension.has_display_field \
            else dimension.definition.as_(format_dimension_key(dimension.key))

        if force_include:
            include = self.slicer.database.to_char(dimension.definition) \
                .isin([str(x) for x in force_include])

            # Ensure that these values are included
            query = query.orderby(include, order=Order.desc)

        # Order by the dimension definition that the choices are for
        query = query.orderby(definition)

        data = fetch_data(self.slicer.database, [query], self._dimensions)

        df_key = format_dimension_key(getattr(dimension, 'display_key', None))
        if df_key is not None:
            return data[df_key]

        display_key = 'display'
        if hasattr(dimension, 'display_values'):
            # Include provided display values
            data[display_key] = pd.Series(dimension.display_values)
        else:
            data[display_key] = data.index.tolist()

        return data[display_key]


class DimensionLatestQueryBuilder(QueryBuilder):
    def __init__(self, slicer):
        super(DimensionLatestQueryBuilder, self).__init__(slicer, slicer.hint_table or slicer.table)

    @immutable
    def __call__(self, dimension: Dimension, *dimensions: Dimension):
        self._dimensions += [dimension] + list(dimensions)

    @property
    def queries(self):
        """
        Serializes this query builder as a set of SQL queries.  This method will always return a list of one
        query since
        only one query is required to retrieve dimension choices.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything
        needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.
        """
        if not self._dimensions:
            raise QueryException('Must select at least one dimension to query latest values')

        query = make_latest_query(database=self.slicer.database,
                                  base_table=self.table,
                                  joins=self.slicer.joins,
                                  dimensions=self._dimensions)
        return [query]

    def fetch(self, hint=None):
        data = super().fetch(hint=hint).reset_index().ix[0]
        # Remove the row index as the name and trim the special dimension key characters from the dimension key
        data.name = None
        data.index = [key[3:] for key in data.index]
        return data
