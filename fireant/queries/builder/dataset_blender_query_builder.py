import copy
from typing import List

from pypika import JoinType, Query

from fireant.queries.builder.dataset_query_builder import DataSetQueryBuilder
from fireant.queries.finders import (
    find_dataset_fields,
    find_field_in_modified_field,
    find_metrics_for_widgets,
    find_operations_for_widgets,
    find_share_dimensions,
)
from fireant.queries.sql_transformer import make_slicer_query_with_totals_and_references
from fireant.reference_helpers import reference_type_alias
from fireant.utils import alias_selector, listify, ordered_distinct_list_by_attr
from fireant.widgets.base import Widget
from ..sets import (
    apply_set_dimensions,
    omit_set_filters,
)


@listify
def _find_dataset_fields_needed_to_be_mapped(dataset):
    """
    This produces a list of fields from a DatasetBlender that need to be mapped up to the Datasets. This is any simple
    fields, or fields that are just pointers to fields in a Dataset, and breaks up complex fields, fields that are
    defined with expressions referencing simple fields from the datasets, down to those fields.

    Blender
        primary
            DS0
                Fields
                    dimension0
                    metric0
        secondary
            DS1
                Fields
                    dimension0
                    metric1
        Fields
            dimension0  (points to dimension0 in DS0 and DS1)
            metric0  (points to metric0 in DS0)
            metric1  (points to metric0 in DS1)
            complex_metric DS0.metric0/DS1.metric1 (Composed of fields metric0 and metric1)

    Calling this function for each of the following will yield
    DS0 -> [DS0.dimension0, DS0.metric0]
    DS1 -> [DS1.dimension0, DS1.metric1]
    Blender -> [Blender.dimension0, Blender.metric0, Blender.metric1, DS0.metric0, DS1.metric1]
        (the Blender has a reference to each field in each DS and also a complex metric composed of metric0 and metric1)
    """
    complex_fields = []
    for field in dataset.fields:
        if field.is_wrapped:
            yield field
        else:
            complex_fields.append(field)

    yield from find_dataset_fields(complex_fields)


@listify
def _map_field(dataset_fields, blender_dataset_fields, dimension_map=None):
    """
    Returns a list of tuples shaped as blended dataset field and its matching field in the provided fields. Blender
    fields that have to match in the dataset fields will be ignored. Please note that the Field class has a custom
    dunder hash method.

    :param dataset_fields: A set of Field instances pertaining to a single dataset (i.e. primary or secondary).
    :param blender_dataset_fields: A list of Field instances in the blender dataset.
    :param dimension_map: A dict mapping Field instances from different datasets.
                          Often used to express the equivalency of primary and secondary dataset fields.
    :return: A list of tuples shaped as blended dataset field and its matching field in the provided fields.
    """
    for field in blender_dataset_fields:
        if dimension_map is not None and field.definition in dimension_map:
            yield field, dimension_map[field.definition]
        if field.definition in dataset_fields:
            yield field, field.definition
            # also yield the dataset field mapped to itself so that any reference to this field (from blender or
            # from dataset) can be used.
            yield field.definition, field.definition
        if field in dataset_fields:
            yield field, field


def _datasets_and_field_maps(blender_dataset, filters):
    """
    Returns a list of tuples shaped as dataset and its fields mapped per the blender dataset ones. The field maps
    are later used for knowing which columns to query on the respective primary and secondary datasets, given the
    columns selected for the blender dataset.

    :param blender_dataset: A DataSetBlender instance.
    :param filters: A list of Filter instances, present on the blender query. This is used for creating dimensions
                    derived from filters, such as the set dimension.
    :return: A list of tuples shaped as dataset and its fields mapped per the blender dataset ones.
    """
    from fireant.dataset.data_blending import DataSetBlender

    def _flatten_blend_datasets(dataset) -> List:
        primary_dataset = dataset.primary_dataset
        secondary_dataset = dataset.secondary_dataset
        blender_dataset_fields = apply_set_dimensions(
            _find_dataset_fields_needed_to_be_mapped(dataset), filters, dataset,
        )

        primary_dataset_fields = set(apply_set_dimensions(primary_dataset.fields, filters, primary_dataset))
        secondary_dataset_fields = set(apply_set_dimensions(secondary_dataset.fields, filters, secondary_dataset))

        blender_field_to_primary_field_map = dict(_map_field(
            primary_dataset_fields, blender_dataset_fields,
        ))

        primary_mapped_dimensions = list(
            apply_set_dimensions(dataset.dimension_map.keys(), filters, primary_dataset)
        )
        secondary_mapped_dimensions = list(
            apply_set_dimensions(dataset.dimension_map.values(), filters, secondary_dataset)
        )
        secondary_dimension_per_primary_dimension = dict(zip(
            primary_mapped_dimensions, secondary_mapped_dimensions,
        ))

        blender_field_to_secondary_field_map = dict(
            _map_field(secondary_dataset_fields, blender_dataset_fields, secondary_dimension_per_primary_dimension)
        )

        if not isinstance(primary_dataset, DataSetBlender):
            return [
                (primary_dataset, blender_field_to_primary_field_map),
                (secondary_dataset, blender_field_to_secondary_field_map),
            ]

        # get the dataset children of the blender (`dataset.primary_dataset`) and their corresponding field_maps,
        # then update the field map to reference this blender's field (`dataset`)
        datasets_and_field_maps = []
        for ds, fm in _flatten_blend_datasets(primary_dataset):
            remapped_field_map = {**fm}
            for field in blender_dataset_fields:
                if (
                    field not in blender_field_to_primary_field_map
                    or blender_field_to_primary_field_map[field] not in fm
                ):
                    continue
                remapped_field_map[field] = fm[blender_field_to_primary_field_map[field]]

            datasets_and_field_maps.append((ds, remapped_field_map))

        return [*datasets_and_field_maps, (secondary_dataset, blender_field_to_secondary_field_map)]

    return zip(*_flatten_blend_datasets(blender_dataset))


class EmptyWidget(Widget):
    @property
    def metrics(self):
        if 0 == len(self.items):
            return []

        return super().metrics


def _build_dataset_query(
    dataset, field_map, metrics, dimensions, filters, references, operations, share_dimensions
):
    @listify
    def _map_fields(fields):
        """
        TODO describe this
        """
        for field in fields:
            field_from_blender = find_field_in_modified_field(field)
            if field_from_blender in dataset.fields:
                yield field
                continue
            if field_from_blender not in field_map:
                continue

            yield field.for_(field_map[field_from_blender])

    dataset_metrics = ordered_distinct_list_by_attr(_map_fields(metrics))
    dataset_dimensions = _map_fields(dimensions)
    dataset_filters = _map_fields(omit_set_filters(filters))
    dataset_references = _map_fields(references)
    dataset_share_dimensions = _map_fields(share_dimensions)

    if not any([dataset_metrics, dataset_dimensions]):
        return [None]

    # TODO: It's possible that we have to adapt/map the operations for @apply_special_cases
    dataset_operations = operations

    return make_slicer_query_with_totals_and_references(
        database=dataset.database,
        table=dataset.table,
        joins=dataset.joins,
        dimensions=dataset_dimensions,
        metrics=dataset_metrics,
        operations=dataset_operations,
        filters=dataset_filters,
        references=dataset_references,
        orders=[],
        share_dimensions=dataset_share_dimensions,
    )


def _blender_join_criteria(
    base_query, join_query, dimensions, base_field_map, join_field_map
):
    """
    Build a criteria for joining this join query to the base query in dataset blender queries. This should be a set of
    equality conditions like A0=B0 AND A1=B1 AND An=Bn for each mapped dimension between dataset from
    `DataSetBlender.dimension_map`.
    """
    join_criteria = None
    for dimension in dimensions:
        dimension = find_field_in_modified_field(dimension)
        # dimension has to be in both field maps:
        if dimension not in base_field_map or dimension not in join_field_map:
            continue

        alias0, alias1 = [
            alias_selector(field_map[dimension].alias)
            for field_map in [base_field_map, join_field_map]
        ]

        next_criteria = base_query[alias0] == join_query[alias1]
        join_criteria = (
            next_criteria if join_criteria is None else (join_criteria & next_criteria)
        )

    return join_criteria


def _get_sq_field_for_blender_field(field, queries, field_maps, reference=None):
    unmodified_field = find_field_in_modified_field(field)
    field_alias = alias_selector(reference_type_alias(field, reference))

    # search for the field in each field map to determine which subquery it will be in
    for query, field_map in zip(queries, field_maps):
        if query is None or unmodified_field not in field_map:
            continue

        mapped_field = field_map[unmodified_field]
        mapped_field_alias = alias_selector(
            reference_type_alias(mapped_field, reference)
        )

        subquery_field = query[mapped_field_alias]
        # case #1 modified fields, ex. day(timestamp) or rollup(dimension)
        return field.for_(subquery_field).as_(field_alias)

    # Need to copy the metrics if there are references so that the `get_sql` monkey patch does not conflict
    definition = copy.deepcopy(field.definition)
    # case #2: complex blender fields
    return definition.as_(field_alias)


def _perform_join_operations(dimensions, base_query, base_field_map, join_queries, join_field_maps):
    blender_query = Query.from_(base_query, immutable=False)
    for join_sql, join_field_map in zip(join_queries, join_field_maps):
        if join_sql is None:
            continue

        criteria = _blender_join_criteria(
            base_query, join_sql, dimensions, base_field_map, join_field_map
        )

        # In most cases there are dimensions to join the two data blending queries on, but if there are none, then
        # instead of doing a join, add the data blending query to the from clause
        blender_query = (
            blender_query.from_(join_sql)  # <-- no dimensions mapped
            if criteria is None
            else blender_query.join(join_sql, JoinType.left).on(
                criteria
            )  # <-- mapped dimensions
        )

    return blender_query


def _blend_query(dimensions, metrics, orders, field_maps, queries):

    # Remove None's in the beginning and take the first non-empty query as base query
    for i, query in enumerate(queries):
        if query:
            base_query, *join_queries = queries[i:]
            base_field_map, *join_field_maps = field_maps[i:]
            break

    reference = base_query._references[0] if base_query._references else None

    if len(queries) == 1:
        # Optimization step, we don't need to do any joining as there is only one query
        blender_query = base_query
    else:
        blender_query = _perform_join_operations(dimensions, base_query, base_field_map, join_queries, join_field_maps)

        # WARNING: In order to make complex fields work, the get_sql for each field is monkey patched in. This must
        # happen here because a complex metric by definition references values selected from the dataset subqueries.

        for metric in find_dataset_fields(metrics):
            subquery_field = _get_sq_field_for_blender_field(metric, queries, field_maps, reference)
            metric.get_sql = subquery_field.get_sql

        sq_dimensions = [_get_sq_field_for_blender_field(d, queries, field_maps) for d in dimensions]
        sq_metrics = [_get_sq_field_for_blender_field(m, queries, field_maps, reference) for m in metrics]
        blender_query = blender_query.select(*sq_dimensions).select(*sq_metrics)

    for field, orientation in orders:
        if any(dimension is field for dimension in dimensions):
            # Don't add the reference type to dimensions
            orderby_field = _get_sq_field_for_blender_field(field, queries, field_maps)
        else:
            orderby_field = _get_sq_field_for_blender_field(field, queries, field_maps, reference)

        blender_query = blender_query.orderby(orderby_field, order=orientation)

    return blender_query


class DataSetBlenderQueryBuilder(DataSetQueryBuilder):
    """
    Blended dataset queries consist of widgets, dimensions, filters, orders by and references. At least one or
    more widgets is required. All others are optional.
    """

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

        datasets, field_maps = _datasets_and_field_maps(self.dataset, self._filters)
        dimensions = self.dimensions
        metrics = find_metrics_for_widgets(self._widgets)
        metrics_aliases = {metric.alias for metric in metrics}
        dimensions_aliases = {dimension.alias for dimension in dimensions}

        orders = self.orders
        if orders is None:
            orders = self.default_orders

        # Add fields to be ordered on, to metrics if they aren't yet selected in metrics or dimensions
        for field, orientation in orders:
            if (
                field.alias not in metrics_aliases
                and field.alias not in dimensions_aliases
            ):
                metrics.append(field)

        dataset_metrics = find_dataset_fields(metrics)
        operations = find_operations_for_widgets(self._widgets)
        share_dimensions = find_share_dimensions(dimensions, operations)

        datasets_queries = []
        for dataset, field_map in zip(datasets, field_maps):
            datasets_queries.append(
                _build_dataset_query(
                    dataset,
                    field_map,
                    dataset_metrics,
                    dimensions,
                    self._filters,
                    self._references,
                    operations,
                    share_dimensions
                )
            )

        """
        A dataset query can yield one or more sql queries, depending on how many types of references or dimensions 
        with totals are selected. A blended dataset query must yield the same number and types of sql queries, but each
        blended together. The individual dataset queries built above will always yield the same number of sql queries, 
        so here those lists of sql queries are zipped.
        
               base   ref  totals ref+totals
        ds1 | ds1_a  ds1_b  ds1_c   ds1_d  
        ds2 | ds2_a  ds2_b  ds2_c   ds2_d  
        
        More concretely, using the diagram above as a reference, a dataset query with 1 reference and 1 totals dimension
        would yield 4 sql queries. With data blending with 1 reference and 1 totals dimension, 4 sql queries must also 
        be produced.  The following lines convert the list of rows of the table in the diagram to a list of columns.
        Each set of queries in a column are then reduced to a single data blending sql query.
        """

        # TODO: There is most likely still a bug when lots of references and total calculations get mixed.
        # Although I haven't been able to actually find a case
        per_dataset_queries_count = max(
            [len(dataset_queries) for dataset_queries in datasets_queries]
        )
        query_sets = [[] for _ in range(per_dataset_queries_count)]

        for dataset_queries in datasets_queries:
            for i, query_set in enumerate(query_sets):
                if len(dataset_queries) > i:
                    query_set.append(dataset_queries[i])

        blended_queries = []
        for queryset in query_sets:
            blended_query = _blend_query(
                dimensions, metrics, orders, field_maps, queryset
            )
            blended_query = self._apply_pagination(blended_query)

            if blended_query:
                blended_queries.append(blended_query)

        return blended_queries
