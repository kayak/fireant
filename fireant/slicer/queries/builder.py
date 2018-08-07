import pandas as pd
from pypika import (
    Order,
)
from typing import (
    Dict,
    Iterable,
)

from fireant.utils import (
    format_dimension_key,
    format_metric_key,
    immutable,
)
from . import special_cases
from .database import fetch_data
from .finders import (
    find_and_group_references_for_dimensions,
    find_and_replace_reference_dimensions,
    find_dimensions_with_totals,
    find_metrics_for_widgets,
    find_operations_for_widgets,
)
from .makers import (
    make_orders_for_dimensions,
    make_slicer_query,
    make_slicer_query_with_references_and_totals,
)
from .references import reference_key
from ..base import SlicerElement
from ..dimensions import Dimension


class QueryBuilder(object):
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
    def query(self):
        """
        Serialize a pypika/SQL query from the slicer builder query.

        This is the base implementation shared by two implementations: the query to fetch data for a slicer request and
        the query to fetch choices for dimensions.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.
        """
        raise NotImplementedError()


class SlicerQueryBuilder(QueryBuilder):
    """
    WRITEME
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
    def orderby(self, element: SlicerElement, orientation=None):
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
    def query(self):
        """
        Build the pypika query for this Slicer query. This collects all of the metrics in each widget, dimensions, and
        filters and builds a corresponding pypika query to fetch the data.  When references are used, the base query
        normally produced is wrapped in an outer query and a query for each reference is joined based on the referenced
        dimension shifted.
        """
        # First run validation for the query on all widgets
        for widget in self._widgets:
            if hasattr(widget, 'validate'):
                widget.validate(self._dimensions)

        self._references = find_and_replace_reference_dimensions(self._references, self._dimensions)
        reference_groups = find_and_group_references_for_dimensions(self._references)
        totals_dimensions = find_dimensions_with_totals(self._dimensions)

        operations = find_operations_for_widgets(self._widgets)
        args = special_cases.apply_to_query_args(self.slicer.database,
                                                 self.table,
                                                 self.slicer.joins,
                                                 self._dimensions,
                                                 find_metrics_for_widgets(self._widgets),
                                                 self._filters,
                                                 reference_groups,
                                                 totals_dimensions,
                                                 operations)

        query = make_slicer_query_with_references_and_totals(*args)

        # Add ordering
        orders = (self._orders or make_orders_for_dimensions(self._dimensions))
        for (term, orientation) in orders:
            query = query.orderby(term, order=orientation)

        return query.limit(self._limit).offset(self._offset)

    def fetch(self, hint=None) -> Iterable[Dict]:
        """
        Fetch the data for this query and transform it into the widgets.

        :param hint:
            A query hint label used with database vendors which support it. Adds a label comment to the query.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        query = self.query
        if hint and hasattr(query, 'hint') and callable(query.hint):
            query = query.hint(hint)

        data_frame = fetch_data(self.slicer.database,
                                str(query),
                                dimensions=self._dimensions)

        # Apply operations
        operations = find_operations_for_widgets(self._widgets)
        for operation in operations:
            for reference in [None] + self._references:
                df_key = format_metric_key(reference_key(operation, reference))
                data_frame[df_key] = operation.apply(data_frame, reference)

        data_frame = special_cases.apply_operations_to_data_frame(operations, data_frame)

        # Apply transformations
        return [widget.transform(data_frame, self.slicer, self._dimensions, self._references)
                for widget in self._widgets]

    def __str__(self):
        return str(self.query)

    def __repr__(self):
        return ".".join(["slicer", "data"]
                        + ["widget({})".format(repr(widget))
                           for widget in self._widgets]
                        + ["dimension({})".format(repr(dimension))
                           for dimension in self._dimensions]
                        + ["filter({})".format(repr(filter))
                           for filter in self._filters]
                        + ["reference({})".format(repr(reference))
                           for reference in self._references])


class DimensionChoicesQueryBuilder(QueryBuilder):
    """
    WRITEME
    """
    def __init__(self, slicer, dimension):
        super(DimensionChoicesQueryBuilder, self).__init__(slicer, slicer.hint_table or slicer.table)
        self._dimensions.append(dimension)

    @property
    def query(self):
        """
        Serialize a pypika/SQL query from the slicer builder query.

        This is the base implementation shared by two implementations: the query to fetch data for a slicer request and
        the query to fetch choices for dimensions.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.
        """

        return make_slicer_query(database=self.slicer.database,
                                 base_table=self.table,
                                 joins=self.slicer.joins,
                                 dimensions=self._dimensions,
                                 filters=self._filters) \
            .limit(self._limit) \
            .offset(self._offset)

    def fetch(self, hint=None, force_include=()) -> pd.Series:
        """
        Fetch the data for this query and transform it into the widgets.

        :param force_include:
            A list of dimension values to include in the result set. This can be used to avoid having necessary results
            cut off due to the pagination.  These results will be returned at the head of the results.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        query = self.query
        if hint and hasattr(query, 'hint') and callable(query.hint):
            query = query.hint(hint)

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

        data = fetch_data(self.slicer.database,
                          str(query),
                          dimensions=self._dimensions)

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
