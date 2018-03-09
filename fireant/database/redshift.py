from pypika import RedshiftQuery

from .postgresql import PostgreSQLDatabase


class RedshiftDatabase(PostgreSQLDatabase):
    """
    Redshift client that uses the psycopg module.
    """
    # The pypika query class to use for constructing queries
    query_cls = RedshiftQuery

    def __init__(self, database=None, host=None, port=5439, user=None, password=None):
        super(RedshiftDatabase, self).__init__(database=database, host=host, port=port,
                                               user=user, password=password)
