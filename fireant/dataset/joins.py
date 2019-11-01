import copy

from pypika import (
    JoinType,
    Table,
    functions as fn,
)

from fireant import Field
from fireant.dataset.modifiers import DimensionModifier
from fireant.utils import alias_selector


def _get_pypika_field_name_or_alias(pypika_field):
    """
    Returns the name or alias, if any, of the provided PyPika Field instance.

    :param pypika_field:
        A PyPika Field instance.
    :return: a string.
    """
    return pypika_field.alias if pypika_field.alias else pypika_field.name


def normalize_join(join, dimensions, metrics):
    """
    Converts and returns a standard Join instance, whenever a specialised Join class is provided.

    :param join:
        A Join instance.
    :param dimensions:
        A list of Field instances.
    :param metrics:
        A list of Field instances that aggregate data.
    :return: a Join instance.
    """
    if isinstance(join, DataSetJoin):
        join = join.join_for_sub_query(dimensions, metrics)

    return join


class Join(object):
    """
    The `Join` class represents a traditional table join in the `Slicer` API. For joining sub-queries see the
    `DataSetJoin` class.
    """

    def __init__(self, table, criterion, join_type=JoinType.inner):
        """
        :param table:
            A PyPika Table instance.
        :param criterion:
            A PyPika boolean expression.
        :param join_type:
            The join type (e.g. inner, left, right). See JoinType enum for more info.
        """
        self.table = table
        self.criterion = criterion
        self.join_type = join_type

    def __repr__(self):
        return '{type} JOIN {table} ON {criterion}'.format(type=self.join_type,
                                                           table=self.table,
                                                           criterion=self.criterion)

    def __gt__(self, other):
        return self.table._table_name < other.table._table_name


class DataSetJoin(Join):
    """
    The `DataSetJoin` class represents a dataset level join in the `Slicer` API.
    """

    def __init__(
        self, primary_dataset, secondary_dataset, join_type=JoinType.inner,
        field_mapping=None, aggregation_func_mapping=None
    ):
        """
        :param primary_dataset:
            A DataSet instance.
        :param secondary_dataset:
            A DataSet instance. The secondary dataset that will be joined as a sub-query.
        :param join_type:
            The join type (e.g. inner, left, right). See JoinType enum for more info.
        :param field_mapping:
            A dictionary with fields in primary dataset as keys and fields in secondary dataset as values.
            If none is provided, one will be generated mapping fields with the same alias. This datastructure
            will be used for generating the join criteria.
        :param aggregation_func_mapping:
            A dictionary with fields in primary dataset as keys and PyPika aggregation functions as values.
            Defaults to Sum for any non provided field.
        """
        self.primary_dataset = primary_dataset
        self.secondary_dataset = secondary_dataset
        self.table = Table('blend_{}'.format(self.secondary_dataset.table._table_name))

        if field_mapping is None:
            field_mapping = {}

            for primary_field in primary_dataset.fields:
                for secondary_field in secondary_dataset.fields:
                    if primary_field.alias == secondary_field.alias:
                        field_mapping[primary_field] = secondary_field

        criterion = None

        for primary_field, secondary_field in field_mapping.items():
            primary_term = self.primary_dataset.table[primary_field.alias]

            if criterion is None:
                criterion = primary_term == secondary_field._dataset_table[secondary_field.alias]
            else:
                criterion = criterion and primary_term == secondary_field._dataset_table[secondary_field.alias]

        self.field_mapping = field_mapping
        self.aggregation_func_mapping = aggregation_func_mapping if aggregation_func_mapping else {}
        self.criterion = criterion
        self.join_type = join_type

    def join_for_sub_query(self, dimensions, metrics):
        """
        Returns a Join instance that a PyPika Query can use to join a subquery, composed of the dimensions and
        metrics that apply to the secondary dataset.

        :param dimensions:
            A list of Field instances.
        :param metrics:
            A list of Field instances that aggregate data.
        :return: a Join instance.
        """
        sub_query = self.__sub_query(dimensions, metrics)
        sub_query_criterion = self.__criterion_for_sub_query(sub_query, dimensions)
        return Join(sub_query, join_type=self.join_type, criterion=sub_query_criterion)

    def __sub_query(self, dimensions, metrics):
        """
        Returns a sub-query that can be used when joining the secondary dataset.

        :param dimensions:
            A list of Field instances.
        :param metrics:
            A list of Field instances that aggregate data.
        :return: a PyPika Query instance.
        """
        sub_query = self.secondary_dataset.sub_query

        secondary_dataset_fields = {
            alias_selector(field.alias): field for field in self.secondary_dataset.fields
        }

        for dimension in dimensions:
            primary_field = dimension if isinstance(dimension, Field) else dimension.dimension

            if primary_field in self.field_mapping:
                if isinstance(dimension, DimensionModifier):
                    new_dimesion = copy.deepcopy(dimension)
                    new_dimesion.dimension = self.field_mapping[primary_field]
                    sub_query = sub_query.dimension(new_dimesion)
                else:
                    sub_query = sub_query.dimension(self.field_mapping[primary_field])

            for pypika_field in primary_field.definition.fields():
                nested_field_alias = _get_pypika_field_name_or_alias(pypika_field)

                if pypika_field.table != self.primary_dataset.table:
                    sub_query = sub_query.dimension(secondary_dataset_fields[nested_field_alias])

        for primary_field in metrics:
            if primary_field in self.field_mapping:
                sub_query = sub_query.dimension(self.field_mapping[primary_field])

            for pypika_field in primary_field.definition.fields():
                nested_field_alias = _get_pypika_field_name_or_alias(pypika_field)

                if pypika_field.table != self.primary_dataset.table:
                    aggregator_func = self.aggregation_func_mapping.get(primary_field, fn.Sum)
                    secondary_dataset_fields[nested_field_alias].definition = aggregator_func(
                        secondary_dataset_fields[nested_field_alias].definition
                    )
                    sub_query = sub_query.dimension(secondary_dataset_fields[nested_field_alias])

        return sub_query.sql.as_(self.table._table_name)

    def __criterion_for_sub_query(self, sub_query, dimensions):
        """
        Returns a PyPika boolean expression, composed of ands involving all the dimensions, which will be used
        as the join criterion for the provided sub-query.

        :param sub_query:
            A PyPika Query instance.
        :param dimensions:
            A list of Field instances.
        :return: a a PyPika boolean expression.
        """
        criterion = None

        for dimension in dimensions:
            primary_field = dimension if isinstance(dimension, Field) else dimension.dimension
            secondary_field = self.field_mapping[primary_field]

            primary_pypika_field = self.primary_dataset.table[
                _get_pypika_field_name_or_alias(primary_field.definition)
            ]
            secondary_pypika_field = getattr(sub_query, alias_selector(secondary_field.alias))

            if criterion is None:
                criterion = primary_pypika_field == secondary_pypika_field
            else:
                criterion &= primary_pypika_field == secondary_pypika_field

        return criterion
