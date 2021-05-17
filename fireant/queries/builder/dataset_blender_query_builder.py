import copy

from typing import List
from pypika import JoinType, terms

from fireant.dataset.fields import Field, is_metric_field
from fireant.queries.builder.dataset_query_builder import DataSetQueryBuilder
from fireant.queries.finders import (
    find_dataset_fields,
    find_field_in_modified_field,
    find_metrics_for_widgets,
    find_operations_for_widgets,
    find_share_dimensions,
)
from fireant.reference_helpers import reference_type_alias
from fireant.utils import alias_selector, filter_nones, listify, ordered_distinct_list_by_attr
from fireant.widgets.base import Widget
from fireant.queries.sets import apply_set_dimensions, omit_set_filters


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
            _find_dataset_fields_needed_to_be_mapped(dataset), filters, dataset
        )

        primary_dataset_fields = set(apply_set_dimensions(primary_dataset.fields, filters, primary_dataset))
        secondary_dataset_fields = set(apply_set_dimensions(secondary_dataset.fields, filters, secondary_dataset))

        blender_field_to_primary_field_map = dict(_map_field(primary_dataset_fields, blender_dataset_fields))

        primary_mapped_dimensions = list(apply_set_dimensions(dataset.dimension_map.keys(), filters, primary_dataset))
        secondary_mapped_dimensions = list(
            apply_set_dimensions(dataset.dimension_map.values(), filters, secondary_dataset)
        )
        secondary_dimension_per_primary_dimension = dict(zip(primary_mapped_dimensions, secondary_mapped_dimensions))

        blender_field_to_secondary_field_map = dict(
            _map_field(
                secondary_dataset_fields,
                blender_dataset_fields,
                secondary_dimension_per_primary_dimension,
            )
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

        return [
            *datasets_and_field_maps,
            (secondary_dataset, blender_field_to_secondary_field_map),
        ]

    return zip(*_flatten_blend_datasets(blender_dataset))


class EmptyWidget(Widget):
    @property
    def metrics(self):
        if 0 == len(self.items):
            return []

        return super().metrics


def map_blender_field_to_dataset_field(field, field_map, dataset):
    field_from_blender = find_field_in_modified_field(field)
    if field_from_blender in dataset.fields:
        return field

    if field_from_blender in field_map:
        return field.for_(field_map[field_from_blender])


def map_blender_fields_to_dataset_fields(fields, field_map, dataset):
    return list(filter_nones(map_blender_field_to_dataset_field(field, field_map, dataset) for field in fields))


def _build_dataset_query(
    dataset,
    field_map,
    dataset_metrics,
    dataset_dimensions,
    dataset_filters,
    references,
    operations,
    share_dimensions,
):
    dataset_references = map_blender_fields_to_dataset_fields(references, field_map, dataset)
    dataset_share_dimensions = map_blender_fields_to_dataset_fields(share_dimensions, field_map, dataset)
    dataset_metrics = ordered_distinct_list_by_attr(dataset_metrics)
    dataset_operations = operations

    return dataset.database.make_slicer_query_with_totals_and_references(
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


def _blender_join_criteria(base_query, join_query, dimensions, base_field_map, join_field_map):
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

        alias0, alias1 = [alias_selector(field_map[dimension].alias) for field_map in [base_field_map, join_field_map]]

        next_criteria = base_query[alias0] == join_query[alias1]
        join_criteria = next_criteria if join_criteria is None else (join_criteria & next_criteria)

    return join_criteria


def _deepcopy_recursive(node):
    if isinstance(node, terms.ValueWrapper):
        return node

    cloned_node = copy.deepcopy(node)

    if hasattr(node, 'definition'):
        cloned_node.definition = _deepcopy_recursive(cloned_node.definition)
    if hasattr(node, 'term'):
        cloned_node.term = _deepcopy_recursive(cloned_node.term)
    if hasattr(node, 'left'):
        cloned_node.left = _deepcopy_recursive(cloned_node.left)
    if hasattr(node, 'right'):
        cloned_node.right = _deepcopy_recursive(cloned_node.right)
    if hasattr(node, 'value'):
        cloned_node.value = _deepcopy_recursive(cloned_node.value)

    # Case expressions
    if hasattr(node, '_cases'):
        cloned_cases = []

        for (criterion, value) in cloned_node._cases:
            cloned_cases.append((_deepcopy_recursive(criterion), _deepcopy_recursive(value)))

        cloned_node._cases = cloned_cases
    if hasattr(node, '_else'):
        cloned_node._else = _deepcopy_recursive(cloned_node._else)

    # Function expressions
    if hasattr(node, 'params'):
        cloned_node.params = [_deepcopy_recursive(param) for param in cloned_node.params]
    if hasattr(node, 'args'):
        cloned_node.args = [_deepcopy_recursive(arg) for arg in cloned_node.args]

    # Modifiers
    if hasattr(cloned_node, 'metric'):
        cloned_node.metric = _deepcopy_recursive(cloned_node.metric)
    if hasattr(cloned_node, 'dimension'):
        cloned_node.dimension = _deepcopy_recursive(cloned_node.dimension)
    if hasattr(cloned_node, 'filter'):
        cloned_node.filter = _deepcopy_recursive(cloned_node.filter)
    if hasattr(cloned_node, 'field'):
        cloned_node.field = _deepcopy_recursive(cloned_node.field)

    return cloned_node


def _get_sq_field_for_blender_field(field, queries, field_maps, reference=None):
    unmodified_field = find_field_in_modified_field(field)
    field_alias = alias_selector(reference_type_alias(field, reference))

    # search for the field in each field map to determine which subquery it will be in
    for query, field_map in zip(queries, field_maps):
        if query is None or unmodified_field not in field_map:
            continue

        mapped_field = field_map[unmodified_field]
        mapped_field_alias = alias_selector(reference_type_alias(mapped_field, reference))

        subquery_field = query[mapped_field_alias]
        # case #1 modified fields, ex. day(timestamp) or rollup(dimension)
        return field.for_(subquery_field).as_(field_alias)

    # Need to copy the metrics if there are references so that the `get_sql` monkey patch does not conflict.
    # Given some of them might have nested metrics themselves, the clone process is performed recursively.

    definition = field.definition

    while isinstance(definition, Field):
        definition = definition.definition

    # case #2: complex blender fields
    return _deepcopy_recursive(definition).as_(field_alias)


def _perform_join_operations(dimensions, base_query, base_field_map, join_queries, join_field_maps):
    blender_query = base_query.QUERY_CLS.from_(base_query, immutable=False)
    for join_sql, join_field_map in zip(join_queries, join_field_maps):
        if join_sql is None:
            continue

        criteria = _blender_join_criteria(base_query, join_sql, dimensions, base_field_map, join_field_map)

        # In most cases there are dimensions to join the two data blending queries on, but if there are none, then
        # instead of doing a join, add the data blending query to the from clause
        blender_query = (
            blender_query.from_(join_sql)  # <-- no dimensions mapped
            if criteria is None
            else blender_query.join(join_sql, JoinType.left).on(criteria)  # <-- mapped dimensions
        )

    return blender_query


def _blend_query(dimensions, metrics, orders, field_maps, queries, query_builder):
    base_query, *join_queries = queries
    base_field_map, *join_field_maps = field_maps

    reference = base_query._references[0] if base_query._references else None
    blender_query = _perform_join_operations(dimensions, base_query, base_field_map, join_queries, join_field_maps)

    mocked_metrics = set()

    # WARNING: In order to make complex fields work, the get_sql for each field is monkey patched in. This must
    # happen here because a complex metric by definition references values selected from the dataset subqueries.
    for metric in find_dataset_fields(metrics):
        subquery_field = _get_sq_field_for_blender_field(metric, queries, field_maps, reference)
        metric._origin_get_sql = metric.get_sql
        metric.get_sql = subquery_field.get_sql
        mocked_metrics.add(metric)

    # WARNING: Artificial dimensions (i.e. dimensions created dynamically), which depend on a metric,
    # can only be properly mapped once the metrics' get_sql methods are monkey patched. That's the case
    # for set dimensions. Therefore, dimensions needs to be re-read from the query builder.
    dimensions = query_builder.dimensions

    sq_dimensions = [_get_sq_field_for_blender_field(d, queries, field_maps) for d in dimensions]
    sq_metrics = [_get_sq_field_for_blender_field(m, queries, field_maps, reference) for m in metrics]

    blender_query = blender_query.select(*sq_dimensions).select(*sq_metrics)

    for field, orientation in orders:
        # Comparing fields using the is operator (i.e. object id) doesn't work for set
        # dimensions, which are dynamically generated. The dunder hash of Field class
        # does the job properly properly though, given set dimensions are treated
        # in particular while object id is used for anything else.
        if not is_metric_field(field):
            # Don't add the reference type to dimensions.
            orderby_field = _get_sq_field_for_blender_field(field, queries, field_maps)
        else:
            orderby_field = _get_sq_field_for_blender_field(field, queries, field_maps, reference)

        blender_query = blender_query.orderby(orderby_field, order=orientation)

    # Undo the get_sql's mocks above, otherwise reused datasets might produce different results. This issue can
    # affect tests, for instance.
    for metric in mocked_metrics:
        metric.get_sql = metric._origin_get_sql

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

        selected_blender_dimensions = self.dimensions
        selected_blender_dimensions_aliases = {dimension.alias for dimension in selected_blender_dimensions}
        selected_blender_metrics = find_metrics_for_widgets(self._widgets)
        selected_blender_metrics_aliases = {metric.alias for metric in selected_blender_metrics}

        operations = find_operations_for_widgets(self._widgets)
        share_dimensions = find_share_dimensions(selected_blender_dimensions, operations)
        non_set_filters = omit_set_filters(self._filters)

        # Add fields to be ordered on, to metrics if they aren't yet selected in metrics or dimensions
        # To think about: if the selected order_by field is a dimension, should we add it to dimensions?
        for field, _ in self.orders:
            if (
                field.alias not in selected_blender_metrics_aliases
                and field.alias not in selected_blender_dimensions_aliases
            ):
                selected_blender_metrics.append(field)

        # Needed dimensions in final query as tuples of (dimension, is_selected_dimension)
        needed_blender_dimensions = [(dimension_field, True) for dimension_field in selected_blender_dimensions]
        # Add dimension filters which are not selected to the pool of needed dimensions
        for filter_ in non_set_filters:
            if not is_metric_field(filter_.field) and (filter_.field.alias not in selected_blender_dimensions_aliases):
                needed_blender_dimensions.append((filter_.field, False))

        selected_metrics_as_dataset_fields = find_dataset_fields(selected_blender_metrics)

        # Determine for each dataset which metrics and dimensions need to be selected
        dataset_dimensions = [[] for _ in range(len(datasets))]
        dataset_metrics = []
        dataset_filters = []
        dataset_included_in_final_query = [False] * len(datasets)

        # First determine the metrics. If a a metric is requested, and the dataset has it, add it for that dataset.
        # We include metrics used in filters. We also save for each dataset the mapped metrics and filters
        for dataset_index, dataset in enumerate(datasets):

            dataset_metrics.append(
                map_blender_fields_to_dataset_fields(
                    selected_metrics_as_dataset_fields,
                    field_maps[dataset_index],
                    dataset,
                )
            )

            dataset_filters.append(
                map_blender_fields_to_dataset_fields(non_set_filters, field_maps[dataset_index], dataset)
            )

            # Metric selected from this dataset, so include it.
            if dataset_metrics[dataset_index]:
                dataset_included_in_final_query[dataset_index] = True
                continue

            # Filter with metric from this dataset selected, so include it.
            for filter_ in dataset_filters[dataset_index]:
                if is_metric_field(filter_.field):
                    dataset_included_in_final_query[dataset_index] = True
                    break

        # Second map the dimensions and find the dimensions which are unique to a dataset. Include those.
        # Also save for each dimension of which datasets it is part of.
        dimensions_dataset_info = []
        for blender_dimension_field, is_selected_dimension in needed_blender_dimensions:
            dimension_dataset_info = []

            for dataset_index, dataset in enumerate(datasets):
                mapped_dimension = map_blender_field_to_dataset_field(
                    blender_dimension_field, field_maps[dataset_index], dataset
                )

                if mapped_dimension is not None:
                    dimension_dataset_info.append((dataset_index, mapped_dimension, is_selected_dimension))

            if len(dimension_dataset_info) == 0:
                # This case should only happen when using sets, otherwise I would have raised the following exception:
                # raise Exception("Dimension requested that was not part of any dataset.")
                pass
            elif len(dimension_dataset_info) == 1:
                # This is the only dataset that has this dimension, assign it
                dataset_index, _, _ = dimension_dataset_info[0]
                dataset_included_in_final_query[dataset_index] = True

            if dimension_dataset_info:
                dimensions_dataset_info.append(dimension_dataset_info)

        # Add all the dimensions to the subqueries that are already selected for the final query
        # Add dimensions that are not yet accounted for to the first dataset that has it
        for dimension_dataset_info in dimensions_dataset_info:
            dimension_accounted_for = False
            first_dataset_that_has_the_dimension = None
            for (dataset_index, mapped_dimension, is_selected_dimension) in dimension_dataset_info:
                # If the dataset is already part of the final query, add this dimension
                if dataset_included_in_final_query[dataset_index]:
                    dimension_accounted_for = True
                    if is_selected_dimension:
                        dataset_dimensions[dataset_index].append(mapped_dimension)

                # Update first_dataset_that_has_the_dimension if needed
                if not dimension_accounted_for and first_dataset_that_has_the_dimension is None:
                    first_dataset_that_has_the_dimension = (
                        dataset_index,
                        mapped_dimension,
                        is_selected_dimension,
                    )

            if not dimension_accounted_for:
                # Dimension not yet accounted for! Take first dataset that has the dimension.
                dataset_index, mapped_dimension, is_selected_dimension = first_dataset_that_has_the_dimension
                dataset_included_in_final_query[dataset_index] = True
                if is_selected_dimension:
                    dataset_dimensions[dataset_index].append(mapped_dimension)

        datasets_queries = []
        filtered_field_maps = []
        for dataset_index, dataset in enumerate(datasets):
            if dataset_included_in_final_query[dataset_index]:
                datasets_queries.append(
                    _build_dataset_query(
                        dataset,
                        field_maps[dataset_index],
                        dataset_metrics[dataset_index],
                        dataset_dimensions[dataset_index],
                        dataset_filters[dataset_index],
                        self._references,
                        operations,
                        share_dimensions,
                    )
                )
                # Filter the field maps of which the dataset is not going to be in the final query.
                filtered_field_maps.append(field_maps[dataset_index])

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

        per_dataset_queries_count = max([len(dataset_queries) for dataset_queries in datasets_queries])
        # There will be the same amount of query sets as the longest length of queries for a single dataset
        query_sets = [[] for _ in range(per_dataset_queries_count)]

        # Add the queries returned for each dataset to the correct queryset
        for dataset_index, dataset_queries in enumerate(datasets_queries):
            for i, query in enumerate(dataset_queries):
                query_sets[i].append(query)

        blended_queries = []
        for queryset in query_sets:
            blended_query = _blend_query(
                selected_blender_dimensions,
                selected_blender_metrics,
                self.orders,
                filtered_field_maps,
                queryset,
                self,
            )
            blended_query = self._apply_pagination(blended_query)

            if blended_query:
                blended_queries.append(blended_query)

        return blended_queries
