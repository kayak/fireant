from typing import (
    Dict,
    Iterable,
)

import pandas as pd

from fireant.dataset.fields import Field
from fireant.dataset.totals import scrub_totals_from_share_results
from fireant.exceptions import SlicerException
from fireant.reference_helpers import reference_alias
from fireant.utils import (
    alias_for_alias_selector,
    alias_selector,
    immutable,
)
from pypika import Order
from . import special_cases
from .execution import fetch_data
from .field_helper import make_orders_for_dimensions
from .finders import (
    find_and_group_references_for_dimensions,
    find_and_replace_reference_dimensions,
    find_metrics_for_widgets,
    find_operations_for_widgets,
    find_share_dimensions,
)
from .pagination import paginate
from .sql_transformer import (
    make_latest_query,
    make_slicer_query,
    make_slicer_query_with_totals_and_references,
)
from ..formats import display_value


class QueryException(SlicerException):
    pass


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

    def __init__(self, dataset, table):
        self.dataset = dataset
        self.table = table
        self._dimensions = []
        self._filters = []
        self._apply_filter_to_totals = []
        self._references = []
        self._limit = None
        self._offset = None

    @immutable
    def filter(self, *filters, apply_to_totals=True):
        """
        :param filters:
        :param apply_to_totals:
        :return:
        """
        self._filters += [f for f in filters]
        self._apply_filter_to_totals += [apply_to_totals] * len(filters)

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


class DataSetQueryBuilder(QueryBuilder):
    """
    Slicer queries consist of widgets, dimensions, filters, and references. At least one or more widgets is required.
    All others are optional.
    """

    def __init__(self, dataset):
        super(DataSetQueryBuilder, self).__init__(dataset, dataset.table)
        self._widgets = []
        self._totals_dimensions = set()
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
        aliases = {dimension.alias
                   for dimension in self._dimensions}
        self._dimensions += [dimension
                             for dimension in dimensions
                             if dimension.alias not in aliases]

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
    def orderby(self, field: Field, orientation: Order = None):
        """
        :param field:
            The element to order by, either a metric or dimension.
        :param orientation:
            The directionality to order by, either ascending or descending.
        :return:
        """
        self._orders += [(field.definition.as_(alias_selector(field.alias)), orientation)]

    def _validate(self):
        for widget in self._widgets:
            if hasattr(widget, 'validate'):
                widget.validate(self._dimensions)

    @property
    def reference_groups(self):
        return list(find_and_group_references_for_dimensions(self._dimensions, self._references).values())

    @property
    def sql(self):
        """
        Serialize this query builder to a list of Pypika/SQL queries. This function will return one query for every
        combination of reference and rolled up dimension (including null options).

        This collects all of the metrics in each widget, dimensions, and filters and builds a corresponding pypika query
        to fetch the data.  When references are used, the base query normally produced is wrapped in an outer query and
        a query for each reference is joined based on the referenced dimension shifted.
        """
        # First run validation for the query on all widgets
        self._validate()

        metrics = find_metrics_for_widgets(self._widgets)
        operations = find_operations_for_widgets(self._widgets)
        share_dimensions = find_share_dimensions(self._dimensions, operations)
        references = find_and_replace_reference_dimensions(self._references, self._dimensions)
        orders = (self._orders or make_orders_for_dimensions(self._dimensions))

        return make_slicer_query_with_totals_and_references(self.dataset.database,
                                                            self.table,
                                                            self.dataset.joins,
                                                            self._dimensions,
                                                            metrics,
                                                            operations,
                                                            self._filters,
                                                            references,
                                                            orders,
                                                            share_dimensions=share_dimensions)

    def fetch(self, hint=None) -> Iterable[Dict]:
        """
        Fetch the data for this query and transform it into the widgets.

        :param hint:
            A query hint label used with database vendors which support it. Adds a label comment to the query.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        queries = add_hints(self.sql, hint)

        operations = find_operations_for_widgets(self._widgets)
        share_dimensions = find_share_dimensions(self._dimensions, operations)

        data_frame = fetch_data(self.dataset.database,
                                queries,
                                self._dimensions,
                                share_dimensions,
                                self.reference_groups)

        # Apply operations
        for operation in operations:
            for reference in [None] + self._references:
                df_key = alias_selector(reference_alias(operation, reference))
                data_frame[df_key] = operation.apply(data_frame, reference)

        data_frame = scrub_totals_from_share_results(data_frame, self._dimensions)
        data_frame = special_cases.apply_operations_to_data_frame(operations, data_frame)
        data_frame = paginate(data_frame,
                              self._widgets,
                              orders=self._orders,
                              limit=self._limit,
                              offset=self._offset)

        # Apply transformations
        return [widget.transform(data_frame, self.dataset, self._dimensions, self._references)
                for widget in self._widgets]

    def __str__(self):
        return str(self.sql)

    def __repr__(self):
        return ".".join(["slicer", "data"]
                        + ["widget({})".format(repr(widget))
                           for widget in self._widgets]
                        + ["dimension({})".format(repr(dimension))
                           for dimension in self._dimensions]
                        + ["filter({}{})".format(repr(f),
                                                 ', apply_filter_to_totals=True' if apply_filter_to_totals else '')
                           for f, apply_filter_to_totals in zip(self._filters, self._apply_filter_to_totals)]
                        + ["reference({})".format(repr(reference))
                           for reference in self._references]
                        + ["orderby({}, {})".format(definition.alias,
                                                    orientation)
                           for (definition, orientation) in self._orders])


class DimensionChoicesQueryBuilder(QueryBuilder):
    """
    This builder is used for building slicer queries for fetching the choices for a dimension given a set of filters.
    """

    def __init__(self, dataset, dimension):
        super(DimensionChoicesQueryBuilder, self).__init__(dataset, dataset.hint_table or dataset.table)
        self._dimensions.append(dimension)

        # TODO remove after 3.0.0
        display_alias = dimension.alias + '_display'
        if display_alias in dataset.fields:
            self._dimensions.append(dataset.fields[display_alias])

    @property
    def sql(self):
        """
        Serializes this query builder as a set of SQL queries.  This method will always return a list of one query since
        only one query is required to retrieve dimension choices.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything needed
        for the query to fetch choices for dimensions.

        The slicer query extends this with metrics, references, and totals.
        """
        query = make_slicer_query(database=self.dataset.database,
                                  base_table=self.table,
                                  joins=self.dataset.joins,
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
        query = add_hints(self.sql, hint)[0]

        dimension = self._dimensions[0]
        definition = dimension.definition.as_(alias_selector(dimension.alias))

        if force_include:
            include = self.dataset.database.to_char(dimension.definition) \
                .isin([str(x) for x in force_include])

            # Ensure that these values are included
            query = query.orderby(include, order=Order.desc)

        # Order by the dimension definition that the choices are for
        query = query.orderby(definition)

        data = fetch_data(self.dataset.database, [query], self._dimensions)

        if len(data.index.names) > 1:
            display_alias = data.index.names[1]
            data.reset_index(display_alias, inplace=True)
            choices = data[display_alias]

        else:
            data['display'] = data.index.tolist()
            choices = data['display']

        dimension_display = self._dimensions[-1]
        return choices.map(lambda raw: display_value(raw, dimension_display) or raw)

    def __repr__(self):
        return ".".join(["slicer", self._dimensions[0].alias, "choices"]
                        + ["filter({})".format(repr(f))
                           for f in self._filters])


class DimensionLatestQueryBuilder(QueryBuilder):
    def __init__(self, dataset):
        super(DimensionLatestQueryBuilder, self).__init__(dataset, dataset.hint_table or dataset.table)

    @immutable
    def __call__(self, dimension: Field, *dimensions: Field):
        self._dimensions += [dimension] + list(dimensions)

    @property
    def sql(self):
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

        query = make_latest_query(database=self.dataset.database,
                                  base_table=self.table,
                                  joins=self.dataset.joins,
                                  dimensions=self._dimensions)
        return [query]

    def fetch(self, hint=None):
        data = super().fetch(hint=hint).reset_index().iloc[0]
        # Remove the row index as the name and trim the special dimension key characters from the dimension key
        data.name = None
        data.index = [alias_for_alias_selector(alias)
                      for alias in data.index]
        return data
