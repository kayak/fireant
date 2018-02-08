from collections import defaultdict
from typing import (
    Dict,
    Iterable,
)

import pandas as pd
from pypika import (
    Order,
    functions as fn,
)
from pypika.enums import SqlTypes
from toposort import (
    CircularDependencyError,
    toposort_flatten,
)

from fireant.slicer.base import SlicerElement
from fireant.utils import (
    flatten,
    immutable,
    ordered_distinct_list,
    ordered_distinct_list_by_attr,
)
from .database import fetch_data
from .references import join_reference
from ..exceptions import (
    CircularJoinsException,
    MissingTableJoinException,
)
from ..filters import DimensionFilter
from ..operations import Operation


def _build_dimension_definition(dimension, interval_func):
    if hasattr(dimension, 'interval'):
        return interval_func(dimension.definition, dimension.interval) \
            .as_(dimension.key)

    return dimension.definition.as_(dimension.key)


def _select_groups(terms, query, rollup, database):
    query = query.select(*terms)

    if not rollup:
        return query.groupby(*terms)

    if 1 < len(terms):
        # This step packs multiple terms together so that they are rolled up together. This is needed for unique
        # dimensions to keep the display field grouped together with the definition field.
        terms = [terms]

    return database.totals(query, terms)


def is_rolling_up(dimension, rolling_up):
    return rolling_up or getattr(dimension, "is_rollup", False)


class QueryBuilder(object):
    def __init__(self, slicer, table):
        self.slicer = slicer
        self.table = table
        self._dimensions = []
        self._filters = []

    @immutable
    def filter(self, *filters):
        """
        :param filters:
        :return:
        """
        self._filters += filters

    @property
    def _elements(self):
        return flatten([self._dimensions, self._filters])

    @property
    def _tables(self):
        """
        Collect all the tables from all of the definitions of all of the elements in the slicer query. This looks
        through the metrics, dimensions, and filter included in this slicer query. It also checks both the definition
        field of each element as well as the display definition for Unique Dimensions.

        :return:
            A collection of tables required to execute a query,
        """
        return ordered_distinct_list([table
                                      for element in self._elements
                                      # Need extra for-loop to incl. the `display_definition` from `UniqueDimension`
                                      for attr in [getattr(element, 'definition', None),
                                                   getattr(element, 'display_definition', None)]
                                      # ... but then filter Nones since most elements do not have `display_definition`
                                      if attr is not None
                                      for table in attr.tables_])

    @property
    def _joins(self):
        """
        Given a set of tables required for a slicer query, this function finds the joins required for the query and
        sorts them topologically.

        :return:
            A list of joins in the order that they must be joined to the query.
        :raises:
            MissingTableJoinException - If a table is required but there is no join for that table
            CircularJoinsException - If there is a circular dependency between two or more joins
        """
        dependencies = defaultdict(set)
        slicer_joins = {join.table: join
                        for join in self.slicer.joins}
        tables_to_eval = list(self._tables)

        while tables_to_eval:
            table = tables_to_eval.pop()

            if self.table == table:
                continue

            if table not in slicer_joins:
                raise MissingTableJoinException('Could not find a join for table {table}'
                                                .format(table=str(table)))

            join = slicer_joins[table]
            tables_required_for_join = set(join.criterion.tables_) - {self.table, join.table}

            dependencies[join] |= {slicer_joins[table]
                                   for table in tables_required_for_join}
            tables_to_eval += tables_required_for_join - {d.table for d in dependencies}

        try:
            return toposort_flatten(dependencies)
        except CircularDependencyError as e:
            raise CircularJoinsException(str(e))

    @property
    def query(self):
        """
        WRITEME
        """
        query = self.slicer.database.query_cls.from_(self.table)

        # Add joins
        for join in self._joins:
            query = query.join(join.table, how=join.join_type).on(join.criterion)

        # Add dimensions
        rolling_up = False
        for dimension in self._dimensions:
            rolling_up = is_rolling_up(dimension, rolling_up)

            dimension_definition = _build_dimension_definition(dimension, self.slicer.database.trunc_date)

            if dimension.has_display_field:
                # Add display definition field
                dimension_display_definition = dimension.display_definition.as_(dimension.display_key)
                fields = [dimension_definition, dimension_display_definition]

            else:
                fields = [dimension_definition]

            query = _select_groups(fields, query, rolling_up, self.slicer.database)

        # Add filters
        for filter_ in self._filters:
            query = query.where(filter_.definition) \
                if isinstance(filter_, DimensionFilter) \
                else query.having(filter_.definition)

        return query


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
    def orderby(self, element: SlicerElement, orientation=None):
        """
        :param element:
            The element to order by, either a metric or dimension.
        :param orientation:
            The directionality to order by, either ascending or descending.
        :return:
        """
        self._orders += [(element.definition.as_(element.key), orientation)]

    @property
    def metrics(self):
        """
        :return:
            an ordered, distinct list of metrics used in all widgets as part of this query.
        """
        return ordered_distinct_list_by_attr([metric
                                              for widget in self._widgets
                                              for metric in widget.metrics])

    @property
    def operations(self):
        """
        :return:
            an ordered, distinct list of metrics used in all widgets as part of this query.
        """
        return ordered_distinct_list_by_attr([operation
                                              for widget in self._widgets
                                              for operation in widget.operations])

    @property
    def orders(self):
        if self._orders:
            return self._orders

        definitions = [dimension.display_definition.as_(dimension.display_key)
                       if dimension.has_display_field
                       else dimension.definition.as_(dimension.key)
                       for dimension in self._dimensions]

        return [(definition, None)
                for definition in definitions]


    @property
    def _elements(self):
        return flatten([self.metrics, self._dimensions, self._filters])

    @property
    def query(self):
        """
        Build the pypika query for this Slicer query. This collects all of the metrics in each widget, dimensions, and
        filters and builds a corresponding pypika query to fetch the data.  When references are used, the base query
        normally produced is wrapped in an outer query and a query for each reference is joined based on the referenced
        dimension shifted.
        """

        # Validate
        for widget in self._widgets:
            if hasattr(widget, 'validate'):
                widget.validate(self._dimensions)

        query = super(SlicerQueryBuilder, self).query

        # Add metrics
        query = query.select(*[metric.definition.as_(metric.key)
                               for metric in self.metrics])

        # Add references
        references = [(reference, dimension)
                      for dimension in self._dimensions
                      for reference in getattr(dimension, 'references', ())]
        if references:
            query = self._join_references(query, references)

        # Add ordering
        for (definition, orientation) in self.orders:
            query = query.orderby(definition, order=orientation)

        return query

    def _join_references(self, query, references):
        """
        This converts the pypika query built in `self.query()` into a query that includes references. This is achieved
        by wrapping the original query with an outer query using the original query as the FROM clause, then joining
        copies of the original query for each reference, with the reference dimension shifted by the appropriate
        interval.

        The outer query selects everything from the original query and each metric from the reference query using an
        alias constructed from the metric key appended with the reference key.  For Delta references, the reference
        metric is selected as the difference of the metric from the original query and the reference query. For Delta
        Percentage references, the reference metric metric is selected as the difference divided by the reference
        metric.

        :param query:
            The original query built by `self.query`
        :param references:
            A list of the references that should be included.
        :return:
            A new pypika query with the dimensions and metrics included from the original query plus each of the
            metrics for each of the references.
        """
        original_query = query.as_('base')

        def original_query_field(key):
            return original_query.field(key).as_(key)

        outer_query = self.slicer.database.query_cls.from_(original_query)

        # Add dimensions
        for dimension in self._dimensions:
            outer_query = outer_query.select(original_query_field(dimension.key))

            if dimension.has_display_field:
                outer_query = outer_query.select(original_query_field(dimension.display_key))

        # Add metrics
        outer_query = outer_query.select(*[original_query_field(metric.key)
                                           for metric in self.metrics])

        # Build nested reference queries
        for reference, dimension in references:
            outer_query = join_reference(reference,
                                         self.metrics,
                                         self._dimensions,
                                         dimension,
                                         self.slicer.database.date_add,
                                         original_query,
                                         outer_query)

        return outer_query

    def fetch(self, limit=None, offset=None) -> Iterable[Dict]:
        """
        Fetch the data for this query and transform it into the widgets.

        :param limit:
            A limit on the number of database rows returned.
        :param offset:
            A offset on the number of database rows returned.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        query = self.query.limit(limit).offset(offset)

        data_frame = fetch_data(self.slicer.database,
                                str(query),
                                dimensions=self._dimensions)

        # Apply operations
        for operation in self.operations:
            data_frame[operation.key] = operation.apply(data_frame)

        # Apply transformations
        return [widget.transform(data_frame, self.slicer, self._dimensions)
                for widget in self._widgets]

    def __str__(self):
        return self.query

    def __iter__(self):
        return iter(self.render())


class DimensionOptionQueryBuilder(QueryBuilder):
    def __init__(self, slicer, dimension):
        super(DimensionOptionQueryBuilder, self).__init__(slicer, slicer.hint_table or slicer.table)
        self._dimensions.append(dimension)

    def fetch(self, limit=None, offset=None, force_include=()) -> pd.Series:
        """
        Fetch the data for this query and transform it into the widgets.

        :param limit:
            A limit on the number of database rows returned.
        :param offset:
            A offset on the number of database rows returned.
        :param force_include:
            A list of dimension values to include in the result set. This can be used to avoid having necessary results
            cut off due to the pagination.  These results will be returned at the head of the results.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        query = self.query

        dimension = self._dimensions[0]
        definition = dimension.display_definition.as_(dimension.display_key) \
            if dimension.has_display_field \
            else dimension.definition.as_(dimension.key)

        if force_include:
            include = fn.Cast(dimension.definition, SqlTypes.VARCHAR) \
                .isin([str(x) for x in force_include])

            # Ensure that these values are included
            query = query.orderby(include, order=Order.desc)

        # Add ordering and pagination
        query = query.orderby(definition).limit(limit).offset(offset)

        data = fetch_data(self.slicer.database,
                          str(query),
                          dimensions=self._dimensions)

        display_key = getattr(dimension, 'display_key', 'display')
        if hasattr(dimension, 'display_values'):
            # Include provided display values
            data[display_key] = pd.Series(dimension.display_values)

        return data[display_key]
