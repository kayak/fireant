from copy import deepcopy
from functools import reduce
from typing import (
    Callable,
    List,
)

from fireant.exceptions import DataSetException
from fireant.dataset.fields import Field
from fireant.queries.builder.dataset_query_builder import DataSetQueryBuilder
from fireant.queries.finders import (
    find_dataset_metrics,
    find_metrics_for_widgets,
)
from fireant.reference_helpers import reference_alias
from fireant.utils import alias_selector
from fireant.widgets.base import Widget
from pypika import (
    JoinType,
    Query,
)


def _datasets_and_field_maps(blender):
    def _flatten_blend_datasets(dataset) -> List:
        primary = dataset.primary_dataset
        secondary = dataset.secondary_dataset

        if hasattr(primary, "primary_dataset"):
            return [
                *_flatten_blend_datasets(primary),
                (secondary, dataset.field_map),
            ]

        return [(primary, None), (secondary, dataset.field_map)]

    return zip(*_flatten_blend_datasets(blender))


def _replace_field(dimension, field_map, omit_umapped=False):
    if hasattr(dimension, "dimension"):
        # Handle modified dimensions
        wrapped_dimension = _replace_field(dimension.dimension, field_map)
        return dimension.for_(wrapped_dimension)

    if field_map is None:
        return dimension.definition

    if not omit_umapped and dimension not in field_map:
        raise DataSetException(
            "Invalid Dimension {}. Dimensions must be mapped in order to be used in a blender query.".format(
                dimension.alias
            )
        )

    return field_map.get(dimension, None)


def _replace_subquery_field(field, field_subquery_map):
    field_alias = alias_selector(field.alias)
    return field_subquery_map[field].as_(field_alias)


class EmptyWidget(Widget):
    pass


def _build_dataset_query(dataset, field_map, metrics, dimensions, filters, references):
    dataset_metrics = [metric for metric in metrics if metric in dataset.fields]

    # Important: If no metrics are needed from this data set, then do not query
    if not dataset_metrics:
        return None

    blended_dimensions = [
        _replace_field(dimension, field_map) for dimension in dimensions
    ]

    blended_filters = []
    for fltr in filters:
        filter_field = _replace_field(fltr.field, field_map, omit_umapped=True)

        if filter_field not in dataset.fields:
            continue

        blended_filters.append(fltr.for_(filter_field))

    blended_references = []
    for reference in references:
        reference_field = _replace_field(reference.field, field_map)

        if reference_field not in dataset.fields:
            continue

        blended_references.append(reference.for_(reference_field))

    return (
        dataset.query()
        .widget(EmptyWidget(*dataset_metrics))
        .dimension(*blended_dimensions)
        .filter(*blended_filters)
        .reference(*blended_references)
    )


def _join_criteria_for_blender_subqueries(primary, secondary, dimensions, field_map):
    join_criteria = []

    for dimension in dimensions:
        mapped_dimension = _replace_field(dimension, field_map)
        p_alias = alias_selector(dimension.alias)
        s_alias = alias_selector(mapped_dimension.alias)
        join_criteria.append(primary[p_alias] == secondary[s_alias])

    return reduce(lambda a, b: a & b, join_criteria)


def _blender(dimensions, metrics, orders, field_maps) -> Callable:
    raw_dataset_metrics = set(find_dataset_metrics(metrics))

    if orders is None:
        orders = [(dimension, None) for dimension in dimensions]

    def _field_subquery_map(dataset_sql):
        """
        This nasty little function returns a dictionary that tells how how to select dimensions and metrics in the
        wrapping blender query using the nested sub-queries.
        """
        base = dataset_sql[0]

        # In sql_transformer#make_slicer_query_with_totals_and_references, a list of references are stored on the Query
        # instance that the query is meant for.
        reference = base._references[0] if base._references else None

        field_subquery_map = {}

        for dimension in dimensions:
            dimension_alias = alias_selector(dimension.alias)
            field_subquery_map[dimension] = base[dimension_alias].as_(dimension_alias)

        # dataset metrics
        for metric in raw_dataset_metrics:
            for sql in dataset_sql:
                metric_alias = alias_selector(reference_alias(metric, reference))
                select_aliases = {select.alias for select in sql._selects}
                if metric_alias in select_aliases:
                    field_subquery_map[metric] = sql[metric_alias].as_(metric_alias)
                    break

        # complex metrics - fields with definitions referring to other fields
        complex_metrics = [
            metric for metric in metrics if metric not in raw_dataset_metrics
        ]
        for metric in complex_metrics:
            # ######### WARNING: This is pretty shitty. #########
            # A `get_sql` method is monkey patched to the instance of each Field inside the definition of the Field
            # containing them. The definition must also be deep copied in case there are reference queries,
            # since there will be multiple instances of the field with different aliases.
            #
            # A better solution for this would be to implement a replace function in pypika which could replace
            # specific nodes in the object graph.
            definition_copy = deepcopy(metric.definition)
            metric_alias = alias_selector(reference_alias(metric, reference))
            field_subquery_map[metric] = definition_copy.as_(metric_alias)
            for field in definition_copy.find_(Field):
                field.get_sql = field_subquery_map[field].get_sql

        return field_subquery_map

    def _blend_query(base, *rest):
        query = Query.from_(base)

        last_sql = base
        for join_sql, field_map in zip(rest, field_maps[1:]):
            join = query.join(join_sql, JoinType.left)
            criteria = _join_criteria_for_blender_subqueries(
                last_sql, join_sql, dimensions, field_map
            )
            query = join.on(criteria)

            # Because DataSetBlenders are chained off of one another, the joins need to also be chained
            last_sql = join_sql

        field_subquery_map = _field_subquery_map([base, *rest])
        query = query.select(
            *[field_subquery_map[select] for select in [*dimensions, *metrics]]
        )

        for field, orientation in orders:
            orderby_term = field_subquery_map[field]
            query = query.orderby(orderby_term, order=orientation)

        return query

    return _blend_query


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

        datasets, field_maps = _datasets_and_field_maps(self.dataset)
        metrics = find_metrics_for_widgets(self._widgets)
        raw_dataset_metrics = find_dataset_metrics(metrics)
        dataset_queries = [
            _build_dataset_query(
                dataset,
                field_map,
                raw_dataset_metrics,
                self._dimensions,
                self._filters,
                self._references,
            )
            for dataset, field_map in zip(datasets, field_maps)
        ]
        # If no metrics are needed from a data set, then remove it from the
        dataset_queries = [q for q in dataset_queries if q is not None]

        # If data blending is unnecessary, then behave just like a regular dataset
        if len(dataset_queries) == 1:
            return dataset_queries[0].sql

        """
        A dataset query can yield one or more sql queries, dependending on how many types of references or dimensions 
        with totals are selected. A blended dataset query must yield the same number and types of sql queries, but each
        blended together. The individual dataset queries built above will always yield the same number of sql queries, 
        so here those lists of sql queries are zipped.
        
               base   ref  totals ref+totals
        ds1 | ds1_a  ds1_b  ds1_c   ds1_d  
        ds2 | ds2_a  ds2_b  ds2_c   ds2_d  
        
        More concretely, using the diagram above as a reference, a dataset query with 1 reference and 1 totals dimension
        would yield 4 sql queries. With data blending with 1 reference and 1 totals dimension, 4 sql queries must also 
        be produced.  This next line converts the list of rows of the table in the diagram to a list of columns. Each 
        set of queries in a column are then reduced to a single data blending sql query. 
        """
        tx_query_matrix = list(
            zip(*[dataset_query.sql for i, dataset_query in enumerate(dataset_queries)])
        )

        blend_query = _blender(self._dimensions, metrics, self._orders, field_maps)
        return [blend_query(*cp) for cp in tx_query_matrix]

    def __str__(self):
        return str(self.sql)
