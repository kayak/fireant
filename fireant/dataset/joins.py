from pypika import JoinType


class Join(object):
    """
    The `Join` class represents a traditional table join in the `DataSet` API. For joining sub-queries see the
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
        return "{type} JOIN {table} ON {criterion}".format(
            type=self.join_type, table=self.table, criterion=self.criterion
        )

    def __gt__(self, other):
        return self.table._table_name < other.table._table_name
