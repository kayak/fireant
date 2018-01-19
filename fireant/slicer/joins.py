from pypika import JoinType


class Join(object):
    """
    WRITEME
    """

    def __init__(self, table, criterion, join_type=JoinType.inner):
        self.table = table
        self.criterion = criterion
        self.join_type = join_type
