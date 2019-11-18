import copy

from pypika import (
    JoinType,
    Table,
    functions as fn,
)
from pypika.terms import AggregateFunction

from fireant import (
    Field,
    Join,
)
from fireant.dataset.modifiers import DimensionModifier
from fireant.queries import DataSetQueryBuilder
from fireant.utils import alias_selector
from ..field_helper import (
    make_orders_for_dimensions,
)
from ..finders import (
    find_and_replace_reference_dimensions,
    find_metrics_for_widgets,
    find_operations_for_widgets,
    find_share_dimensions,
)
from ..sql_transformer import (
    make_slicer_query_with_totals_and_references,
)


def _get_pypika_field_name_or_alias(pypika_field):
    """
    Returns the name or alias, if any, of the provided PyPika Field instance.

    :param pypika_field:
        A PyPika Field instance.
    :return: a string.
    """
    return pypika_field.alias if pypika_field.alias else pypika_field.name


class DataSetBlenderQueryBuilder(DataSetQueryBuilder):
    """
    Blended slicer queries consist of widgets, dimensions, filters, orders by and references. At least one or
    more widgets is required. All others are optional.
    """

    def __init__(self, dataset, aggregation_func_mapping=None):
        super(DataSetBlenderQueryBuilder, self).__init__(dataset)
        self.primary_dataset = dataset.primary_dataset
        self.secondary_datasets = dataset.secondary_datasets
        self.all_datasets = [self.primary_dataset, *self.secondary_datasets]

        if aggregation_func_mapping is None:
            # SUM is used by default when no mapping is found
            aggregation_func_mapping = {}

        self.aggregation_func_mapping = aggregation_func_mapping

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
        references = find_and_replace_reference_dimensions(self._references, self._dimensions)
        orders = (self._orders or make_orders_for_dimensions(self._dimensions))

        sub_query_joins = set()
        sub_query_tables = {}

        primary_table = self.__primary_sub_query(self._dimensions, metrics, self._filters)

        # Map all blend_table_name tables to the actual sub query (i.e. PyPika query) that should be used
        sub_query_tables[self.primary_dataset.table] = primary_table
        sub_query_tables[Table('blend_{}'.format(self.primary_dataset.table._table_name))] = primary_table

        for secondary_dataset in self.dataset.secondary_datasets:
            sub_query_join = self.__join_for_sub_query(
                primary_table, secondary_dataset, self._dimensions, metrics, self._filters,
            )
            sub_query_joins.add(sub_query_join)
            sub_query_tables[secondary_dataset.table] = sub_query_join.table
            sub_query_tables[Table('blend_{}'.format(secondary_dataset.table._table_name))] = sub_query_join.table

        # Use the aforementioned map to replace mapped tables in provided dimensions, metrics and filters
        for dimension in self._dimensions:
            sub_query_tables[dimension._pypika_field_for_referencing_sub_queries.table] = sub_query_tables[
                dimension.definition.table
            ]
            sub_query = sub_query_tables[dimension.definition.table]
            new_definition = copy.deepcopy(dimension.definition)
            new_definition.table = sub_query
            dimension.definition = new_definition

        for metric in metrics:
            for pypika_field in metric.definition.fields():
                sub_query = sub_query_tables[pypika_field.table]
                pypika_field.table = sub_query

        for filter in self._filters:
            for pypika_field in filter.definition.fields():
                sub_query = sub_query_tables[pypika_field.table]
                pypika_field.table = sub_query

        # Unwrap dimension modifiers on wrapper query level, since modifiers were already applied on sub-queries
        dimensions = []

        for dimension in self._dimensions:
            new_dimension = dimension
            if isinstance(dimension, DimensionModifier):
                # We only need to apply dimension modifiers in the from and join levels
                new_dimension = new_dimension.dimension

            dimensions.append(new_dimension)

        return make_slicer_query_with_totals_and_references(self.dataset,
                                                            self.dataset.primary_dataset.database,
                                                            primary_table,
                                                            [join for join in sub_query_joins],
                                                            dimensions,
                                                            metrics,
                                                            operations,
                                                            self._filters,
                                                            references,
                                                            orders,
                                                            share_dimensions=share_dimensions)

    def __str__(self):
        return str(self.sql)

    def __repr__(self):
        return ".".join(["slicer", "data"]
                        + ["widget({})".format(repr(widget))
                           for widget in self._widgets]
                        + ["dimension({})".format(repr(dimension))
                           for dimension in self._dimensions]
                        + ["filter({})".format(repr(f))
                           for f in self._filters]
                        + ["reference({})".format(repr(reference))
                           for reference in self._references]
                        + ["orderby({}, {})".format(definition.alias,
                                                    orientation)
                           for (definition, orientation) in self._orders])

    def __primary_sub_query(self, dimensions, metrics, filters):
        """
        Returns a Join instance that a PyPika Query can use to join a subquery, composed of the dimensions and
        metrics that apply to the secondary dataset.

        :param dimensions:
            A list of Field instances.
        :param metrics:
            A list of Field instances that aggregate data.
        :param filters:
            A list of Filter instances.
        :return: a PyPika Query instance.
        """
        return self.__sub_query(self.dataset.primary_dataset, dimensions, metrics, filters)

    def __join_for_sub_query(self, primary_table_sub_query, secondary_dataset, dimensions, metrics, filters):
        """
        Returns a Join instance that a PyPika Query can use to join a subquery, composed of the dimensions and
        metrics that apply to the secondary dataset.

        :param primary_table_sub_query:
            a PyPika Query instance.
        :param secondary_dataset:
            A dataset instance.
        :param dimensions:
            A list of Field instances.
        :param metrics:
            A list of Field instances that aggregate data.
        :param filters:
            A list of Filter instances.
        :return: a Join instance.
        """
        sub_query = self.__sub_query(secondary_dataset, dimensions, metrics, filters)
        sub_query_criterion, dimensions_without_mapping = self.__criterion_for_sub_query(
            primary_table_sub_query, secondary_dataset, sub_query, dimensions,
        )
        return Join(sub_query, join_type=JoinType.left, criterion=sub_query_criterion)

    def __sub_query(self, dataset, dimensions, metrics, filters):
        """
        Returns a sub-query that can be used when joining the secondary dataset.

        :param dataset:
            A dataset instance.
        :param dimensions:
            A list of Field instances.
        :param metrics:
            A list of Field instances that aggregate data.
        :param filters:
            A list of Filter instances.
        :return: a PyPika Query instance.
        """
        sub_query = dataset.sub_query
        field_mapping = self.dataset.field_mapping[dataset.table._table_name]

        secondary_dataset_fields = {
            alias_selector(field.alias): field for ds in [dataset] for field in ds.fields
        }

        for dimension in dimensions:
            primary_field = dimension if isinstance(dimension, Field) else dimension.dimension

            if primary_field in field_mapping:
                if isinstance(dimension, DimensionModifier):
                    new_dimension = dimension
                    new_dimension.dimension = field_mapping[primary_field]
                    sub_query = sub_query.dimension(new_dimension)
                else:
                    sub_query = sub_query.dimension(field_mapping[primary_field])

            for pypika_field in primary_field.definition.fields():
                nested_field_alias = alias_selector(_get_pypika_field_name_or_alias(pypika_field))

                if nested_field_alias not in secondary_dataset_fields:
                    continue

                sub_query = sub_query.dimension(secondary_dataset_fields[nested_field_alias])

        for primary_field in metrics:
            if primary_field in field_mapping:
                sub_query = sub_query.dimension(field_mapping[primary_field])

            for pypika_field in primary_field.definition.fields():
                nested_field_alias = alias_selector(_get_pypika_field_name_or_alias(pypika_field))

                if nested_field_alias not in secondary_dataset_fields:
                    continue

                aggregator_func = self.aggregation_func_mapping.get(primary_field, fn.Sum)
                new_definition = copy.deepcopy(secondary_dataset_fields[nested_field_alias].definition)

                if isinstance(new_definition, AggregateFunction):
                    for definition_pypika_field in new_definition.fields():
                        definition_pypika_field.table = copy.deepcopy(definition_pypika_field.table)
                    secondary_dataset_fields[nested_field_alias].definition = new_definition
                else:
                    new_definition.table = copy.deepcopy(new_definition.table)
                    secondary_dataset_fields[nested_field_alias].definition = (
                        aggregator_func(new_definition)
                    )
                sub_query = sub_query.dimension(secondary_dataset_fields[nested_field_alias])

        pypika_query = sub_query.sql

        for primary_filter in filters:
            new_filter = copy.deepcopy(primary_filter)
            nested_field_alias = alias_selector(new_filter.field_alias)

            if nested_field_alias not in secondary_dataset_fields:
                continue

            new_criterion = copy.deepcopy(primary_filter.definition)
            new_criterion.term = secondary_dataset_fields[nested_field_alias].definition
            new_filter.definition = new_criterion

            pypika_query = pypika_query.having(new_filter.definition) \
                if new_filter.is_aggregate \
                else pypika_query.where(new_filter.definition)

        return pypika_query

    def __criterion_for_sub_query(self, primary_table_sub_query, dataset, sub_query, dimensions):
        """
        Returns a PyPika boolean expression, composed of ands involving all the dimensions, which will be used
        as the join criterion for the provided sub-query.

        :param primary_table_sub_query:
            a PyPika Query instance.
        :param dataset:
            A dataset instance.
        :param sub_query:
            A PyPika Query instance.
        :param dimensions:
            A list of Field instances.
        :return: a a PyPika boolean expression.
        """
        criterion = None
        dimensions_without_mapping = []
        field_mapping = self.dataset.field_mapping[dataset.table._table_name]

        for dimension in dimensions:
            primary_field = dimension if isinstance(dimension, Field) else dimension.dimension
            try:
                secondary_field = field_mapping[primary_field]
            except KeyError:
                dimensions_without_mapping.append(dimension)
                continue

            primary_pypika_field = getattr(
                primary_table_sub_query, _get_pypika_field_name_or_alias(primary_field.definition),
            )
            secondary_pypika_field = getattr(sub_query, alias_selector(secondary_field.alias))

            if criterion is None:
                criterion = primary_pypika_field == secondary_pypika_field
            else:
                criterion &= primary_pypika_field == secondary_pypika_field

        return criterion, dimensions_without_mapping
