from pypika import JoinType


class Join(object):
    """
    WRITEME
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
