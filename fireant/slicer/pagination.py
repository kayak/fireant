class Paginator(object):
    def __init__(self, limit=0, offset=0, order=()):
        """
        Class for keeping track of pagination parameters

        :param limit: The number of rows that should be returned
        :param offset: The number of rows to offset the query by
        :param order: Collection of tuples in the format
                      (<metric/dimension key>, ``pypika.Order.desc`` or ``pypika.Order.asc``)
        """
        self.offset = offset
        self.limit = limit
        self.order = order

    def __str__(self):
        return 'offset: {offset} limit: {limit} order: {order}'.format(offset=self.offset,
                                                                       limit=self.limit,
                                                                       order=[(key, orderby.name)
                                                                              for key, orderby in self.order])
