from pypika import JoinType

from ..exceptions import SlicerException


class Join(object):
    """
    The `Join` class represents a traditional table join in the `Slicer` API. For joining sub-queries see the
    `DataSetJoin` class.
    """

    def __init__(self, table, criterion, join_type=JoinType.inner):
        self.table = table
        self.criterion = criterion
        self.join_type = join_type

    def __repr__(self):
        return '{type} JOIN {table} ON {criterion}'.format(type=self.join_type,
                                                           table=self.table,
                                                           criterion=self.criterion)

    def __gt__(self, other):
        return self.table._table_name < other.table._table_name

    def __hash__(self):
        return hash((self.table, self.criterion, self.join_type))


class QueryJoin(object):
    """
    The `DataSetJoin` class is a specialised version of the `Join` class, which is meant for joining sub-queries.
    """
    @classmethod
    def query(cls, dataset_query_builder, alias=None):
        """
        Returns a Pypika's Query subclass instance for the database specified on the provided DataSetQueryBuilder
        instance.

        :param dataset_query_builder: a DataSetQueryBuilder instance.
        :param alias: an alias. Defaults to the dataset_query_builder's table name.
        :return: a Pypika's Query subclass instance.
        """
        if dataset_query_builder.references or dataset_query_builder.widgets:
            raise SlicerException(
                'The provided DataSetQueryBuilder instance cannot have references and/or widgets,'
                'when used in a QueryJoin'
            )

        query = dataset_query_builder.sql[0]
        query = query.as_(alias or dataset_query_builder.table._table_name)

        return query

    def __init__(self, query, criterion, join_type=JoinType.inner):
        self.query = query
        self.dataset = query.dataset
        self.criterion = criterion
        self.join_type = join_type

    def __repr__(self):
        return '{type} JOIN ({sub_query}) {alias} ON {criterion}'.format(type=self.join_type,
                                                                         sub_query=self.item,
                                                                         alias=self.item.alias,
                                                                         criterion=self.criterion)

    def __gt__(self, other):
            return self.dataset.table._table_name < other.table._table_name

    def __hash__(self):
        return hash((self.query, self.criterion, self.join_type))

    @property
    def table(self):
        return self.query
