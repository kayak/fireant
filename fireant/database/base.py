from datetime import datetime
from functools import partial
from typing import Collection, Dict, Union
from typing import Iterable, List, Sequence, Type

import pandas as pd
from pypika import (
    Query,
    enums,
    terms,
)
from pypika import Table, functions as fn
from pypika.queries import QueryBuilder
from pypika.terms import Function

from fireant.dataset.fields import Field
from fireant.dataset.filters import Filter
from fireant.dataset.joins import Join
from fireant.exceptions import QueryCancelled
from fireant.middleware.decorators import apply_middlewares, connection_middleware
from fireant.queries.finders import (
    find_totals_dimensions,
    find_and_group_references_for_dimensions,
    find_required_tables_to_join,
    find_joins_for_tables,
)
from fireant.queries.references import make_reference_dimensions, make_reference_metrics, make_reference_filters
from fireant.queries.special_cases import adjust_daterange_filter_for_rolling_window
from fireant.queries.totals_helper import adapt_for_totals_query
from fireant.utils import (
    alias_selector,
    flatten,
)
from fireant.dataset.intervals import DatetimeInterval


class Database(object):
    """
    This is a abstract base class used for interfacing with a database platform.
    """

    # The pypika query class to use for constructing queries
    query_cls = Query

    slow_query_log_min_seconds = 15

    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        max_result_set_size=200000,
        middlewares=[],
    ):
        self.host = host
        self.port = port
        self.database = database
        self.max_result_set_size = max_result_set_size
        self.middlewares = middlewares + [connection_middleware]

    def connect(self):
        """
        This function must establish a connection to the database platform and return it.
        """
        raise NotImplementedError

    def cancel(self, connection):
        """
        Cancel any running query.
        """
        if hasattr(connection, "cancel"):
            connection.cancel()
        else:
            # A default cancel for databases for which no specific cancel is implemented
            # This will force an exit of the connection context manager
            raise QueryCancelled("Query was cancelled")

    def get_column_definitions(self, schema, table, connection=None):
        """
        Return a list of column name, column data type pairs.

        :param schema: The name of the table schema.
        :param table: The name of the table to get columns from.
        :param connection: (Optional) The connection to execute this query with.
        :return: A list of columns.
        """
        raise NotImplementedError

    def trunc_date(self, field, interval):
        """
        This function must create a Pypika function which truncates a Date or DateTime object to a specific interval.
        """
        raise NotImplementedError

    def date_add(self, field: terms.Term, date_part: str, interval: int):
        """
        This function must add/subtract a Date or Date/Time object.
        """
        raise NotImplementedError

    def convert_date(self, dt: datetime) -> Union[datetime, Function]:
        """
        Override to provide a custom function for converting a date.
        Defaults to an identity function.

        :param dt: Date to convert
        """
        return dt

    def to_char(self, definition):
        return fn.Cast(definition, enums.SqlTypes.VARCHAR)

    @apply_middlewares
    def fetch_queries(self, *queries, connection=None, parameters: Union[Dict, Collection] = ()):
        results = []
        # Parameters can either be passed as a list when using formatting placeholders like %s (varies per platform)
        # or a dict when using named placeholders.
        for query in queries:
            cursor = connection.cursor()
            cursor.execute(str(query), parameters)
            results.append(cursor.fetchall())

        return results

    def fetch(self, query, **kwargs):
        return self.fetch_queries(query, **kwargs)[0]

    @apply_middlewares
    def execute(self, *queries, **kwargs):
        connection = kwargs.get("connection")
        for query in queries:
            cursor = connection.cursor()
            cursor.execute(str(query))
            connection.commit()

    @apply_middlewares
    def fetch_dataframes(self, *queries, parse_dates=None, **kwargs):
        connection = kwargs.get("connection")
        dataframes = []
        for query in queries:
            dataframes.append(pd.read_sql(query, connection, coerce_float=True, parse_dates=parse_dates))
        return dataframes

    def fetch_dataframe(self, query, **kwargs):
        return self.fetch_dataframes(query, **kwargs)[0]

    def __str__(self):
        return f'Database|{self.__class__.__name__}|{self.host}'

    def make_slicer_query_with_totals_and_references(
        self,
        table,
        joins,
        dimensions,
        metrics,
        operations,
        filters,
        references,
        orders,
        share_dimensions=(),
    ) -> List[Type[QueryBuilder]]:
        """
        The following two loops will run over the spread of the two sets including a NULL value in each set:
         - reference group (WoW, MoM, etc.)
         - dimension with roll up/totals enabled (totals dimension)

        This will result in at least one query where the reference group and totals dimension is NULL, which shall be
        called base query. The base query will ALWAYS be present, even if there are zero reference groups or totals
        dimensions.

        For a concrete example, check the test case in :
        ```
        fireant.tests.queries.test_build_dimensions.QueryBuilderDimensionTotalsTests
            #test_build_query_with_totals_cat_dimension_with_references
        ```
        """

        filters = adjust_daterange_filter_for_rolling_window(dimensions, operations, filters)

        totals_dimensions = find_totals_dimensions(
            dimensions,
            share_dimensions,
        )
        totals_dimensions_and_none = [None] + totals_dimensions[::-1]

        reference_groups = find_and_group_references_for_dimensions(dimensions, references)
        reference_groups_and_none = [(None, None)] + list(reference_groups.items())

        queries = []
        for totals_dimension in totals_dimensions_and_none:
            (dimensions_with_totals, filters_with_totals) = adapt_for_totals_query(
                totals_dimension,
                dimensions,
                filters,
            )

            for reference_parts, references in reference_groups_and_none:
                (dimensions_with_ref, metrics_with_ref, filters_with_ref,) = self.adapt_for_reference_query(
                    reference_parts,
                    dimensions_with_totals,
                    metrics,
                    filters_with_totals,
                    references,
                )
                query = self.make_slicer_query(
                    table,
                    joins,
                    dimensions_with_ref,
                    metrics_with_ref,
                    filters_with_ref,
                    orders,
                )

                # Add these to the query instance so when the data frames are joined together, the correct references and
                # totals can be applied when combining the separate result set from each query.
                query._totals = totals_dimension
                query._references = references
                queries.append(query)

        return queries

    def adapt_for_reference_query(self, reference_parts, dimensions, metrics, filters, references):
        if reference_parts is None:
            return dimensions, metrics, filters

        ref_dim, unit, interval = reference_parts

        offset_func = partial(self.date_add, date_part=unit, interval=interval)
        offset_func_inv = partial(self.date_add, date_part=unit, interval=-interval)

        ref_dimensions = make_reference_dimensions(
            dimensions, ref_dim, offset_func, self.transform_field_to_query, self.trunc_date
        )
        ref_metrics = make_reference_metrics(metrics, references[0].reference_type.alias)
        ref_filters = make_reference_filters(filters, ref_dim, offset_func_inv)

        return ref_dimensions, ref_metrics, ref_filters

    def make_slicer_query(
        self,
        base_table: Table,
        joins: Sequence[Join] = (),
        dimensions: Sequence[Field] = (),
        metrics: Sequence[Field] = (),
        filters: Sequence[Filter] = (),
        orders: Sequence = (),
    ) -> Type[QueryBuilder]:
        """
        Creates a pypika/SQL query from a list of slicer elements.

        This is the base implementation shared by two implementations: the query to fetch data for a slicer request and
        the query to fetch choices for dimensions.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.

        :param base_table:
            pypika.Table - The base table of the query, the one in the FROM clause
        :param joins:
            A collection of joins available in the slicer. This should include all slicer joins. Only joins required for
            the query will be used.
        :param dimensions:
            A collection of dimensions to use in the query.
        :param metrics:
            A collection of metrics to use in the query.
        :param filters:
            A collection of filters to apply to the query.
        :param orders:
            A collection of orders as tuples of the metric/dimension to order by and the direction to order in.

        :return:
        """
        query = self.query_cls.from_(base_table, immutable=False)
        elements = flatten([metrics, dimensions, filters])

        # Add joins
        join_tables_needed_for_query = find_required_tables_to_join(elements, base_table)

        for join in find_joins_for_tables(joins, base_table, join_tables_needed_for_query):
            query = query.join(join.table, how=join.join_type).on(join.criterion)

        # Add dimensions
        for dimension in dimensions:
            dimension_term = self.transform_field_to_query(dimension, self.trunc_date)
            query = query.select(dimension_term)

            if dimension.groupable:
                query = query.groupby(dimension_term)

        # Add filters
        for fltr in filters:
            query = query.having(fltr.definition) if fltr.is_aggregate else query.where(fltr.definition)

        # Add metrics
        metric_terms = [self.transform_field_to_query(metric) for metric in metrics]
        if metric_terms:
            query = query.select(*metric_terms)

        # In the case that the orders are determined by a field that is not selected as a metric or dimension, then it needs
        # to be added to the query.
        select_aliases = {el.alias for el in query._selects}
        for (orderby_field, orientation) in orders:
            orderby_term = self.transform_field_to_query(orderby_field)
            query = query.orderby(orderby_term, order=orientation)

            if orderby_term.alias not in select_aliases:
                query = query.select(orderby_term)

        return query

    def make_latest_query(
        self,
        base_table: Table,
        joins: Iterable[Join] = (),
        dimensions: Iterable[Field] = (),
    ):
        query = self.query_cls.from_(base_table, immutable=False)

        # Add joins
        join_tables_needed_for_query = find_required_tables_to_join(dimensions, base_table)
        for join in find_joins_for_tables(joins, base_table, join_tables_needed_for_query):
            query = query.join(join.table, how=join.join_type).on(join.criterion)

        for dimension in dimensions:
            f_dimension_key = alias_selector(dimension.alias)
            query = query.select(fn.Max(dimension.definition).as_(f_dimension_key))

        return query

    def transform_field_to_query(self, field, window=None):
        """
        Makes a list of pypika terms for a given dataset field.

        :param field:
            A field from a dataset.
        :param window:
            A window function to apply to the dimension definition if it is a continuous dimension.
        :return:
            a list of terms required to select and group by in a SQL query given a dataset dimension. This list will contain
            either one or two elements. A second element will be included if the dimension has a definition for its display
            field.
        """
        f_alias = alias_selector(field.alias)

        if window and isinstance(field, DatetimeInterval):
            return window(field.definition, field.interval_key).as_(f_alias)

        return field.definition.as_(f_alias)
