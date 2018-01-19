from collections import defaultdict

from toposort import (
    CircularDependencyError,
    toposort_flatten,
)

from fireant.utils import (
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
from ..intervals import DatetimeInterval


def _build_dimension_definition(dimension, interval_func):
    if hasattr(dimension, 'interval') and isinstance(dimension.interval, DatetimeInterval):
        return interval_func(dimension.definition,
                             dimension.interval).as_(dimension.key)

    return dimension.definition.as_(dimension.key)


class QueryBuilder(object):
    """

    """

    def __init__(self, slicer):
        self.slicer = slicer
        self._widgets = []
        self._dimensions = []
        self._filters = []
        self._orders = []

    @immutable
    def widget(self, widget):
        """

        :param widget:
        :return:
        """
        self._widgets.append(widget)

    @immutable
    def dimension(self, dimension):
        """

        :param dimension:
        :param rollup:
        :return:
        """
        self._dimensions.append(dimension)

    @immutable
    def filter(self, filter):
        """

        :param filter:
        :return:
        """
        self._filters.append(filter)

    @property
    def tables(self):
        """
        :return:
            A collection of tables required to execute a query,
        """
        return ordered_distinct_list([table
                                      for group in [self.metrics, self._dimensions, self._filters]
                                      for element in group
                                      for attr in [getattr(element, 'definition', None),
                                                   getattr(element, 'display_definition', None)]
                                      if attr is not None
                                      for table in attr.tables_])

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
    def joins(self):
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
        tables_to_eval = list(self.tables)

        while tables_to_eval:
            table = tables_to_eval.pop()

            if self.slicer.table == table:
                continue

            if table not in slicer_joins:
                raise MissingTableJoinException('Could not find a join for table {table}'
                                                .format(table=str(table)))

            join = slicer_joins[table]
            tables_required_for_join = set(join.criterion.tables_) - {self.slicer.table, join.table}

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
        query = self.slicer.database.query_cls.from_(self.slicer.table)

        # Add joins
        for join in self.joins:
            query = query.join(join.table, how=join.join_type).on(join.criterion)

        # Add dimensions
        for dimension in self._dimensions:
            dimension_definition = _build_dimension_definition(dimension, self.slicer.database.trunc_date)
            query = query.select(dimension_definition).groupby(dimension_definition)

            # Add display definition field
            if hasattr(dimension, 'display_definition'):
                dimension_display_definition = dimension.display_definition.as_(dimension.display_key)
                query = query.select(dimension_display_definition).groupby(dimension_display_definition)

        # Add metrics
        query = query.select(*[metric.definition.as_(metric.key)
                               for metric in self.metrics])

        # Add filters
        for filter_ in self._filters:
            query = query.where(filter_.definition) \
                if isinstance(filter_, DimensionFilter) \
                else query.having(filter_.definition)

        # Add references
        references = [(reference, dimension)
                      for dimension in self._dimensions
                      if hasattr(dimension, 'references')
                      for reference in dimension.references]
        if references:
            query = self._join_references(query, references)

        # Add ordering
        order = self._orders if self._orders else self._dimensions
        query = query.orderby(*[element.definition.as_(element.key)
                                for element in order])

        return str(query)

    def _join_references(self, query, references):
        original_query = query.as_('base')

        def original_query_field(key):
            return original_query.field(key).as_(key)

        outer_query = self.slicer.database.query_cls.from_(original_query)

        # Add dimensions
        for dimension in self._dimensions:
            outer_query = outer_query.select(original_query_field(dimension.key))

            if hasattr(dimension, 'display_definition'):
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

    def render(self):
        """

        :return:
        """
        dataframe = fetch_data(self.slicer.database, self.query, index_levels=[dimension.key
                                                                               for dimension in self._dimensions])

        # Apply operations
        ...

        # Apply transformations
        return [widget.transform(dataframe, self.slicer)
                for widget in self._widgets]
