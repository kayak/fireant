from typing import List

from pypika import Order

from fireant.utils import alias_selector
from .query_builder import (
    QueryBuilder,
    add_hints,
    get_column_names,
)
from ..execution import fetch_data
from ..field_helper import make_term_for_field
from ..finders import find_joins_for_tables
from ..sql_transformer import make_slicer_query
from ...formats import display_value


class DimensionChoicesQueryBuilder(QueryBuilder):
    """
    This builder is used for building dataset queries for fetching the choices for a dimension given a set of filters.
    """

    def __init__(self, dataset, dimension):
        super().__init__(dataset)

        self.hint_table = getattr(dimension, "hint_table", None)
        self._dimensions.append(dimension)

        # TODO remove after 3.0.0
        display_alias = dimension.alias + "_display"
        if display_alias in dataset.fields:
            self._dimensions.append(dataset.fields[display_alias])

    def _extract_hint_filters(self):
        """
        Extracts filters that can be applied when using the hint table.

        :return:
            A list of filters.
        """
        base_table = self.dataset.table
        hint_column_names = get_column_names(self.dataset.database, self.hint_table)

        filters = []
        for filter_ in self.filters:
            base_fields = [
                field
                for field in filter_.definition.fields_()
                if all(table == base_table for table in field.tables_)
            ]

            join_tables = [
                table
                for field in filter_.definition.fields_()
                for table in field.tables_
                if table != base_table
            ]

            required_joins = find_joins_for_tables(
                self.dataset.joins, self.dataset.table, join_tables
            )

            base_fields.extend(
                [
                    field
                    for join in required_joins
                    for field in join.criterion.fields_()
                    if all(table == base_table for table in field.tables_)
                ]
            )

            if all(field.name in hint_column_names for field in base_fields):
                filters.append(filter_)

        return filters

    def _make_terms_for_hint_dimensions(self):
        """
        Makes a list pypika terms using the hint table instead of their original table.

        :return:
            A list of pypika terms.
        """
        dimension_terms = []
        for dimension in self.dimensions:
            dimension_term = make_term_for_field(
                dimension, self.dataset.database.trunc_date
            )
            dimension_term = dimension_term.replace_table(
                dimension_term.table, self.hint_table
            )
            dimension_terms.append(dimension_term)

        return dimension_terms

    @property
    def sql(self):
        """
        Serializes this query builder as a set of SQL queries. This method will always return a list of one query since
        only one query is required to retrieve dimension choices.

        The dataset query extends this with metrics, references, and totals.
        """
        dimensions = [] if self.hint_table else self.dimensions

        filters = self._extract_hint_filters() if self.hint_table else self.filters

        query = (
            make_slicer_query(
                database=self.dataset.database,
                base_table=self.dataset.table,
                joins=self.dataset.joins,
                dimensions=dimensions,
                filters=filters,
            )
            .limit(self._query_limit)
            .offset(self._query_offset)
        )

        if self.hint_table:
            hint_dimension_terms = self._make_terms_for_hint_dimensions()
            query = query.select(*hint_dimension_terms).groupby(*hint_dimension_terms)
            query = query.replace_table(self.dataset.table, self.hint_table)

        return [query]

    def fetch(self, hint=None, force_include=()) -> List[str]:
        """
        Fetch the data for this query and transform it into the widgets.

        :param hint:
            For database vendors that support it, add a query hint to collect analytics on the queries triggered by
            fireant.
        :param force_include:
            A list of dimension values to include in the result set. This can be used to avoid having necessary results
            cut off due to the pagination.  These results will be returned at the head of the results.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        query = add_hints(self.sql, hint)[0]
        dimension = self.dimensions[0]
        alias_definition = dimension.definition.as_(alias_selector(dimension.alias))
        dimension_definition = dimension.definition

        if self.hint_table:
            alias_definition = alias_definition.replace_table(
                alias_definition.table, self.hint_table
            )
            dimension_definition = dimension.definition.replace_table(
                dimension_definition.table, self.hint_table
            )

        if force_include:
            include = self.dataset.database.to_char(dimension_definition).isin(
                [str(x) for x in force_include]
            )

            # Ensure that these values are included
            query = query.orderby(include, order=Order.desc)

        # Filter out NULL values from choices
        query = query.where(dimension_definition.notnull())

        # Order by the dimension definition that the choices are for
        query = query.orderby(alias_definition)
        max_rows_returned, data = fetch_data(self.dataset.database, [query], self.dimensions)

        if len(data.index.names) > 1:
            display_alias = data.index.names[1]
            data.reset_index(display_alias, inplace=True)
            choices = data[display_alias]

        else:
            data["display"] = data.index.tolist()
            choices = data["display"]

        dimension_display = self.dimensions[-1]
        choices = choices.map(lambda raw: display_value(raw, dimension_display) or raw)
        return self._transform_for_return(choices, max_rows_returned=max_rows_returned)

    def __repr__(self):
        return ".".join(
            ["dataset", self._dimensions[0].alias, "choices"]
            + ["filter({})".format(repr(f)) for f in self._filters]
        )
