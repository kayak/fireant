import abc

from multiprocessing.pool import ThreadPool


class BaseConcurrencyMiddleware(abc.ABC):
    """
    The abstract base class that should be inherited from to define a concurrency middleware.
    """

    @abc.abstractmethod
    def fetch_query(self, query, database):
        """
        Perform a query on the database.

        :param query: The query to execute.
        :param database: The database to perform the query on.
        :return: The result of the query.
        """
        pass

    @abc.abstractmethod
    def fetch_queries_as_dataframe(self, queries, database):
        """
        Implementations of this method should execute the given queries on the supplied database and return the results.
        :return: A list of the results of the executed queries.
        """
        pass


class ThreadPoolConcurrencyMiddleware(BaseConcurrencyMiddleware):
    """
    A concurrency middleware implementation based on threadpools used as a default middleware.
    """

    def __init__(self, max_processes=1):
        self.max_processes = max_processes

    def fetch_query(self, query, database):
        return database.fetch(query)

    def fetch_queries_as_dataframe(self, queries, database):
        """
        Executes the different queries in separate threads.
        """
        iterable = [(query, ) for query in queries]

        with ThreadPool(processes=self.max_processes) as pool:
            results = pool.map(lambda args: database.fetch_dataframe(*args), iterable)
            pool.close()

        return results
