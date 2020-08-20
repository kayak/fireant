from typing import Dict, Iterable, List, TYPE_CHECKING, Type, Union

from fireant.dataset.fields import DataType
from fireant.dataset.intervals import DatetimeInterval
from fireant.dataset.totals import scrub_totals_from_share_results
from fireant.reference_helpers import (
    apply_reference_filters,
    reference_alias,
)
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
    find_field_in_modified_field,
    find_metrics_for_widgets,
    find_operations_for_widgets,
    find_share_dimensions,
)
from ..pagination import paginate
from ..sql_transformer import (
    make_slicer_query,
    make_slicer_query_with_totals_and_references,
)

if TYPE_CHECKING:
    from pypika import PyPikaQueryBuilder


class DataSetQueryBuilder(
    ReferenceQueryBuilderMixin, WidgetQueryBuilderMixin, QueryBuilder
):
    """
    Data Set queries consist of widgets, dimensions, filters, orders by and references. At least one or more widgets
    is required. All others are optional.
    """

    def __init__(self, dataset):
        super().__init__(dataset)
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
                self.dimensions, self._references
            ).values()
        )

    @property
    def sql(self) -> List[Type['PyPikaQueryBuilder']]:
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

        dimensions = self.dimensions

        metrics = find_metrics_for_widgets(self._widgets)
        operations = find_operations_for_widgets(self._widgets)
        share_dimensions = find_share_dimensions(dimensions, operations)

        queries = make_slicer_query_with_totals_and_references(
            database=self.dataset.database,
            table=self.table,
            joins=self.dataset.joins,
            dimensions=dimensions,
            metrics=metrics,
            operations=operations,
            filters=self.filters,
            references=self._references,
            orders=self.orders,
            share_dimensions=share_dimensions,
        )

        return [self._apply_pagination(query) for query in queries]

    def fetch(self, hint=None) -> Union[Iterable[Dict], Dict]:
        """
        Fetch the data for this query and transform it into the widgets.

        :param hint:
            A query hint label used with database vendors which support it. Adds a label comment to the query.
        :return:
            A list of dict (JSON) objects containing the widget configurations.
        """
        queries = add_hints(self.sql, hint)

        operations = find_operations_for_widgets(self._widgets)
        dimensions = self.dimensions

        share_dimensions = find_share_dimensions(dimensions, operations)

        annotation_frame = None
        if dimensions and self.dataset.annotation:
            alignment_dimension_alias = (
                self.dataset.annotation.dataset_alignment_field_alias
            )
            first_dimension = find_field_in_modified_field(dimensions[0])

            if first_dimension.alias == alignment_dimension_alias:
                annotation_frame = self.fetch_annotation()

        max_rows_returned, data_frame = fetch_data(
            self.dataset.database,
            queries,
            dimensions,
            share_dimensions,
            self.reference_groups,
        )

        # Apply reference filters
        for reference in self._references:
            data_frame = apply_reference_filters(data_frame, reference)

        # Apply operations
        for operation in operations:
            for reference in [None] + self._references:
                df_key = alias_selector(reference_alias(operation, reference))
                data_frame[df_key] = operation.apply(data_frame, reference)

        data_frame = scrub_totals_from_share_results(data_frame, dimensions)
        data_frame = special_cases.apply_operations_to_data_frame(
            operations, data_frame
        )

        orders = self.orders
        if orders is None:
            orders = self.default_orders

        data_frame = paginate(
            data_frame,
            self._widgets,
            orders=orders,
            limit=self._client_limit,
            offset=self._client_offset,
        )

        # Apply transformations
        widget_data = [
            widget.transform(
                data_frame,
                dimensions,
                self._references,
                annotation_frame,
            )
            for widget in self._widgets
        ]

        return self._transform_for_return(widget_data, max_rows_returned=max_rows_returned)

    def fetch_annotation(self):
        """
        Fetch annotation data for this query builder.

        :return:
            A data frame containing the annotation data.
        """
        annotation = self.dataset.annotation

        # Fetch filters for the dataset's alignment dimension from this query builder
        dataset_alignment_dimension_filters = self.fetch_query_filters(
            annotation.dataset_alignment_field_alias
        )

        # Update fields in filters for the dataset's alignment dimension to the annotation's alignment field
        annotation_alignment_dimension_filters = [
            dataset_alignment_filter.for_(annotation.alignment_field)
            for dataset_alignment_filter in dataset_alignment_dimension_filters
        ]

        annotation_alignment_field = annotation.alignment_field
        if annotation_alignment_field.data_type == DataType.date:
            dataset_alignment_dimension = self.fetch_query_dimension(
                annotation.dataset_alignment_field_alias
            )

            if hasattr(dataset_alignment_dimension, "interval_key"):
                # Use the interval key of the dataset's alignment dimension for the annotation's alignment field
                # Otherwise we would need to copy it to prevent issues from patching directly
                annotation_alignment_field = DatetimeInterval(
                    annotation.alignment_field, dataset_alignment_dimension.interval_key
                )

        annotation_dimensions = [annotation_alignment_field, annotation.field]

        annotation_query = make_slicer_query(
            database=self.dataset.database,
            base_table=annotation.table,
            dimensions=annotation_dimensions,
            filters=annotation_alignment_dimension_filters,
        )

        _, annotation_df = fetch_data(
            self.dataset.database, [annotation_query], [annotation.alignment_field]
        )

        return annotation_df

    def fetch_query_filters(self, dimension_alias):
        """
        Fetch all filters matching the given dimension alias from this query builder. All fields of a filter
        (e.g. complex fields) have to match the alias and use the base table of the dataset.

        :param dimension_alias:
            An alias of a dimension.
        :return:
            A list of filters for the given dimension alias.
        """
        dimension_filters = []

        for filter_ in self.filters:
            filter_fields = [
                field
                for field in filter_.definition.fields_()
                if all(table == self.dataset.table for table in field.tables_)
            ]

            if not filter_fields:
                continue

            if all(field.name == dimension_alias for field in filter_fields):
                dimension_filters.append(filter_)

        return dimension_filters

    def fetch_query_dimension(self, dimension_alias):
        """
        Fetch a dimension matching the given dimension alias from this query builder.

        :param dimension_alias:
            An alias of a dimension.
        :return:
            A dimension (Field) of this query builder matching the dimension alias.
        """
        for dimension in self.dimensions:
            unwrapped_dimension = find_field_in_modified_field(dimension)
            if unwrapped_dimension.alias == dimension_alias:
                return dimension

        return None

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
                    for (definition, orientation) in (self.orders or self.default_orders)
                ],
            ]
        )
