import copy
from collections import (
    OrderedDict,
    defaultdict,
)
from itertools import chain

from pypika import (
    JoinType,
    Table,
)

from fireant import (
    Field,
    Join,
)
from fireant.dataset.modifiers import DimensionModifier
from fireant.queries.builder.dataset_query_builder import DataSetQueryBuilder
from fireant.queries.field_helper import (
    make_orders_for_dimensions,
    make_term_for_metrics,
)
from fireant.queries.finders import (
    find_and_replace_reference_dimensions,
    find_metrics_for_widgets,
)
from fireant.reference_helpers import reference_alias
from fireant.utils import alias_selector
from fireant.widgets.base import Widget


class DataSetBlenderQueryBuilder(DataSetQueryBuilder):
    """
    Blended slicer queries consist of widgets, dimensions, filters, orders by and references. At least one or
    more widgets is required. All others are optional.
    """

    def __init__(self, dataset):
        super(DataSetBlenderQueryBuilder, self).__init__(dataset)
        self.primary_dataset = dataset.primary_dataset
        self.secondary_datasets = dataset.secondary_datasets

    @property
    def all_datasets(self):
        return chain([self.primary_dataset], self.secondary_datasets)

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

        metrics = [metric for metric in find_metrics_for_widgets(self._widgets)]
        orders = (self._orders or make_orders_for_dimensions(self._dimensions))

        sub_query_joins_groups, sub_query_tables_groups = self.__sub_query_groups

        queries = []

        for i, (sub_query_joins, sub_query_tables) in enumerate(zip(sub_query_joins_groups, sub_query_tables_groups)):
            dimension_aliases = {alias_selector(el.alias) for el in self._dimensions}
            metric_aliases = {alias_selector(el.alias) for el in metrics}
            reference_aliases = {
                alias_selector(reference_alias(metric, reference))
                for metric in metrics
                for reference in self._references
            }
            all_aliases = dimension_aliases | metric_aliases | reference_aliases

            field_to_select = OrderedDict()
            aliases_already_added = set()
            primary_table = sub_query_tables[self.primary_dataset.table]

            if i > 0 and self._references:
                # From the second query onwards, we don't need to add the normal metrics, given the equivalent one
                # with the reference applied will be present.
                aliases_already_added.update(metric_aliases)

            query = self.primary_dataset.database.query_cls.from_(primary_table)

            for field in primary_table._selects:
                if field.alias in aliases_already_added or field.alias not in all_aliases:
                    continue
                field_to_select[field.alias] = primary_table
                aliases_already_added.add(field.alias)

            for join in sub_query_joins:
                query = query.join(join.table, how=join.join_type).on(join.criterion)
                secondary_table = join.table

                for field in secondary_table._selects:
                    if field.alias in aliases_already_added or field.alias not in all_aliases:
                        continue
                    field_to_select[field.alias] = secondary_table
                    aliases_already_added.add(field.alias)

            # Add dimensions
            for field_alias, table in field_to_select.items():
                dimension_term = getattr(table, field_alias)
                dimension_term = dimension_term.as_(field_alias)
                query = query.select(dimension_term)

            # Add metrics
            for metric in metrics:
                if alias_selector(metric.alias) not in aliases_already_added:
                    metric_definition = make_term_for_metrics(metric)
                    query = query.select(metric_definition)
                    field_to_select[metric_definition.alias] = query

            # Add filters
            for filter in self._filters:
                if alias_selector(filter.field_alias) not in metric_aliases:
                    continue

                table = field_to_select[alias_selector(filter.field_alias)]

                if filter.is_aggregate:
                    filter.definition.left = getattr(table, alias_selector(filter.field_alias))

                query = query.where(filter.definition)

            # Add orders by
            for (orderby_term, orientation) in orders:
                query = query.orderby(orderby_term, order=orientation)

                if orderby_term.alias not in all_aliases:
                    # In the case that the orders are determined by a field that is not selected as a metric or
                    # dimension, then it needs to be added to the query.
                    query = query.select(orderby_term)

            for table, sub_query in sub_query_tables.items():
                query = query.replace_table(table, sub_query)

            queries.append(query)

        return queries

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
        :return: a PyPika boolean expression.
        """
        criterion = None
        dimensions_without_mapping = []
        field_mapping = self.dataset.field_mapping[dataset.table]

        for dimension in dimensions:
            primary_field = dimension if isinstance(dimension, Field) else dimension.dimension
            try:
                secondary_field = field_mapping[primary_field]
            except KeyError:
                dimensions_without_mapping.append(dimension)
                continue

            primary_pypika_field = getattr(
                primary_table_sub_query, alias_selector(primary_field.alias),
            )
            secondary_pypika_field = getattr(sub_query, alias_selector(secondary_field.alias))

            if criterion is None:
                criterion = primary_pypika_field == secondary_pypika_field
            else:
                criterion &= primary_pypika_field == secondary_pypika_field

        return criterion, dimensions_without_mapping

    @property
    def __queries_per_dataset(self):
        """
        Returns a list of Pypika Query instances with dimensions, metrics, filters and references applied.

        :return: a list of DataSetQuery instances with dimensions, metrics, filters and references applied.
        """
        queries_per_dataset = {}

        for dataset in self.all_datasets:
            queries_per_dataset[dataset.table] = dataset.query

        dimensions_per_dataset = self.__dimensions_per_dataset
        metrics_per_dataset = self.__metrics_per_dataset
        filters_per_dataset = self.__filters_per_dataset
        references_per_dataset = self.__references_per_dataset

        for dataset, dataset_dimensions in dimensions_per_dataset.items():
            for dimension in dataset_dimensions:
                queries_per_dataset[dataset.table] = queries_per_dataset[dataset.table].dimension(dimension)

        for dataset, dataset_metrics in metrics_per_dataset.items():
            if not dataset_metrics:
                continue

            for _ in self._widgets:
                # The widget type does not really matter, given the generated sub-queries are only going to be
                # used by the sql method.
                queries_per_dataset[dataset.table] = queries_per_dataset[dataset.table].widget(Widget(*dataset_metrics))

        for dataset, dataset_filters in filters_per_dataset.items():
            for filter in dataset_filters:
                new_filter = copy.deepcopy(filter)
                queries_per_dataset[dataset.table] = queries_per_dataset[dataset.table].filter(new_filter)

        for dataset, dataset_references in references_per_dataset.items():
            for reference in dataset_references:
                queries_per_dataset[dataset.table] = queries_per_dataset[dataset.table].reference(reference)

        return queries_per_dataset

    @property
    def __sub_query_groups(self):
        """
        Returns a tuple with Join instances and Pypika Query instances as first and second elements respectively.

        :return: a tuple with Join instances and Pypika Query instances as first and second elements respectively.
        """

        queries_per_dataset = self.__queries_per_dataset

        primary_tables = queries_per_dataset[self.primary_dataset.table].sql
        sub_query_tables = [dict() for _ in primary_tables]
        sub_query_joins = [set() for _ in primary_tables]

        for i, primary_table in enumerate(primary_tables):
            sub_query_tables[i][self.primary_dataset.table] = primary_table
            sub_query_tables[i][Table('blend_{}'.format(self.primary_dataset.table._table_name))] = primary_table

            for secondary_dataset in self.secondary_datasets:
                sub_query = queries_per_dataset[secondary_dataset.table].sql[i]
                sub_query_tables[i][secondary_dataset.table] = sub_query
                sub_query_tables[i][Table('blend_{}'.format(secondary_dataset.table._table_name))] = sub_query
                sub_query_criterion, dimensions_without_mapping = self.__criterion_for_sub_query(
                    primary_table, secondary_dataset, sub_query, self._dimensions,
                )

                sub_query_joins[i].add(Join(sub_query, join_type=JoinType.left, criterion=sub_query_criterion))

        return sub_query_joins, sub_query_tables

    @property
    def __dimensions_per_dataset(self):
        """
        Returns a mapping between datasets and their respective dimensions.

        :return: a dictionary with datasets as keys and their respective list of dimensions as values.
        """
        dimensions_per_dataset = defaultdict(list)

        for dimension in self._dimensions:
            for dataset in self.all_datasets:
                new_dimension = copy.deepcopy(dimension)

                if isinstance(dimension, DimensionModifier):
                    new_dimension.dimension = self.dataset.field_mapping[dataset.table][
                        new_dimension.dimension
                    ]

                for pypika_field in new_dimension.definition.fields():
                    pypika_field.table = dataset.table
                dimensions_per_dataset[dataset].append(new_dimension)

        return dimensions_per_dataset

    @property
    def __metrics_per_dataset(self):
        """
        Returns a mapping between datasets and their respective metrics.

        :return: a dictionary with datasets as keys and their respective list of metrics as values.
        """
        metrics_per_dataset = defaultdict(list)
        metrics = find_metrics_for_widgets(self._widgets)

        for metric in metrics:
            for pypika_field in metric.definition.fields():
                for dataset in self.all_datasets:
                    try:
                        metrics_per_dataset[dataset].append(
                            dataset.fields[
                                pypika_field.name.replace('_', '-').replace('$', '')
                            ]
                        )
                    except AttributeError:
                        # We need to ignore a metric when it doesn't exist in the dataset.
                        pass

        return metrics_per_dataset

    @property
    def __filters_per_dataset(self):
        """
        Returns a mapping between datasets and their respective filters.

        :return: a dictionary with datasets as keys and their respective list of filters as values.
        """
        filters_per_dataset = defaultdict(list)

        for filter in self._filters:
            for dataset in self.all_datasets:
                try:
                    dataset.fields[filter.field_alias]
                    new_filter = copy.deepcopy(filter)
                    for pypika_field in new_filter.definition.fields():
                        pypika_field.table = dataset.table
                    filters_per_dataset[dataset].append(new_filter)
                except AttributeError:
                    # We need to ignore a filter referencing a metric, when the respective metric doesn't
                    # exist in the dataset.
                    pass

        return filters_per_dataset


    @property
    def __references_per_dataset(self):
        """
        Returns a mapping between datasets and their respective references.

        :return: a dictionary with datasets as keys and their respective list of references as values.
        """
        references_per_dataset = defaultdict(list)

        for dataset in self.all_datasets:
            references = find_and_replace_reference_dimensions(self._references, dataset.fields)

            for reference in references:
                new_reference = copy.deepcopy(reference)

                if isinstance(reference.field, DimensionModifier):
                    reference.field.dimension = self.dataset.field_mapping[dataset.table][
                        reference.field.dimension
                    ]

                for pypika_field in reference.field.definition.fields():
                    pypika_field.table = dataset.table
                references_per_dataset[dataset].append(new_reference)

        return references_per_dataset
