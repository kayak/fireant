from typing import (
    Dict,
    Iterable,
)

from fireant.dataset.totals import scrub_totals_from_share_results
from fireant.reference_helpers import reference_alias
from fireant.utils import (
    alias_selector,
    immutable,
)

from .query_builder import (
    QueryBuilder,
    QueryException,
    ReferenceQueryBuilderMixin,
    WidgetQueryBuilderMixin,
    add_hints,
)
from .. import special_cases
from ..execution import fetch_data
from ..finders import (
    find_and_group_references_for_dimensions,
    find_metrics_for_widgets,
    find_operations_for_widgets,
    find_share_dimensions,
)
from ..pagination import paginate
from ..sql_transformer import make_slicer_query_with_totals_and_references


class DataSetQueryBuilder(
    ReferenceQueryBuilderMixin, WidgetQueryBuilderMixin, QueryBuilder
):
    """
    Data Set queries consist of widgets, dimensions, filters, orders by and references. At least one or more widgets
    is required. All others are optional.
    """

    def __init__(self, dataset):
        super(DataSetQueryBuilder, self).__init__(dataset, dataset.table)
        self._totals_dimensions = set()
        self._apply_filter_to_totals = []

    def __call__(self, *args, **kwargs):
        return self

    @immutable
    def filter(self, *filters, apply_to_totals=True):
        """
        Add one or more filters when building a dataset query.

        :param filters:
            Filters to add to the query
        :param apply_to_totals:
            Whether filters should apply to totals or not
        :return:
            A copy of the query with the filters added.
        """
        self._filters += [f for f in filters]
        self._apply_filter_to_totals += [apply_to_totals] * len(filters)

    @property
    def reference_groups(self):
        return list(
            find_and_group_references_for_dimensions(
                self._dimensions, self._references
            ).values()
        )

    @property
    def sql(self):
        """
        Serialize this query builder to a list of Pypika/SQL queries. This function will return one query for every
        combination of reference and rolled up dimension (including null options).

        This collects all of the metrics in each widget, dimensions, and filters and builds a corresponding pypika query
        to fetch the data.  When references are used, the base query normally produced is wrapped in an outer query and
        a query for each reference is joined based on the referenced dimension shifted.

        :return: a list of Pypika's Query subclass instances.
        """
        # First run validation for the query on all widgets
        self._validate()

        metrics = find_metrics_for_widgets(self._widgets)
        operations = find_operations_for_widgets(self._widgets)
        share_dimensions = find_share_dimensions(self._dimensions, operations)

        return self.make_query(
            metrics,
            operations,
            share_dimensions,
        )

    def make_query(self, metrics, operations, share_dimensions):
        return make_slicer_query_with_totals_and_references(
            self.dataset.database,
            self.table,
            self.dataset.joins,
            self._dimensions,
            metrics,
            operations,
            self._filters,
            self._references,
            self.orders,
            share_dimensions=share_dimensions,
        )

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

        data_frame = fetch_data(
            self.dataset.database,
            queries,
            self._dimensions,
            share_dimensions,
            self.reference_groups,
        )

        # Apply operations
        for operation in operations:
            for reference in [None] + self._references:
                df_key = alias_selector(reference_alias(operation, reference))
                data_frame[df_key] = operation.apply(data_frame, reference)

        data_frame = scrub_totals_from_share_results(data_frame, self._dimensions)
        data_frame = special_cases.apply_operations_to_data_frame(
            operations, data_frame
        )
        data_frame = paginate(
            data_frame,
            self._widgets,
            orders=self.orders,
            limit=self._limit,
            offset=self._offset,
        )

        # Apply transformations
        return [
            widget.transform(
                data_frame, self.dataset, self._dimensions, self._references
            )
            for widget in self._widgets
        ]

    def plot(self):
        try:
            from IPython.display import display
        except ImportError:
            raise QueryException(
                "Optional dependency ipython missing. Please install fireant[ipython] to use plot."
            )

        widgets = self.fetch()
        for widget in reversed(widgets):
            display(widget)

    def __str__(self):
        return str(self.sql)

    def __repr__(self):
        return ".".join(
            [
                "dataset",
                "query",
                *["widget({})".format(repr(widget)) for widget in self._widgets],
                *[
                    "dimension({})".format(repr(dimension))
                    for dimension in self._dimensions
                ],
                *[
                    "filter({}{})".format(
                        repr(f),
                        ", apply_filter_to_totals=True"
                        if apply_filter_to_totals
                        else "",
                    )
                    for f, apply_filter_to_totals in zip(
                        self._filters, self._apply_filter_to_totals
                    )
                ],
                *[
                    "reference({})".format(repr(reference))
                    for reference in self._references
                ],
                *[
                    "orderby({}, {})".format(definition.alias, orientation)
                    for (definition, orientation) in self.orders
                ],
            ],
        )
