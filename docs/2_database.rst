Database Configuration
======================
Database Connector
------------------
In order for |Brand| to connect to your database, a database connector must be used. This takes the form of an instance of a concrete subclass of |Brand|'s |ClassDatabase| class. Database connectors are shipped with |Brand| for all of the supported databases, but it is also possible to write your own. See below on how to extend |Brand| to support additional databases.

To configure a database, instantiate a subclass of |ClassDatabase|. You will use this instance to create a |FeatureDataSet|. It is possible to use multiple databases simultaneous, but |ClassDataSet| can only use a single database, since they inherently model the structure of a table in the database.

Vertica

.. code-block:: python

    import fireant.settings
    from fireant.database import VerticaDatabase

    database = VerticaDatabase(
        host='example.com',
        port=5433,
        database='example',
        user='user',
        password='password123',
    )

MySQL

.. code-block:: python

    import fireant.settings
    from fireant.database import MySQLDatabase

    database = MySQLDatabase(
        database='testdb',
        host='mysql.example.com',
        port=3308,
        user='user',
        password='password123',
        charset='utf8mb4',
    )

Microsoft SQL Server
.. code-block:: python

    import fireant.settings
    from fireant.database import MSSQLDatabase

    database = MSSQLDatabase(
        database='testdb',
        host='mysql.example.com',
        port=3308,
        user='user',
        password='password123',
    )

Please note, for this connection, you need to install FreeTDS.

PostgreSQL

.. code-block:: python

    import fireant.settings
    from fireant.database import PostgreSQLDatabase

    database = PostgreSQLDatabase(
        database='testdb',
        host='example.com',
        port=5432,
        user='user',
        password='password123',
    )

Amazon Redshift

.. code-block:: python

    import fireant.settings
    from fireant.database import RedshiftDatabase

    fireant.settings = RedshiftDatabase(
        database='testdb',
        host='example.com',
        port=5439,
        user='user',
        password='password123',
    )

Snowflake

.. code-block:: python

    from fireant.database import SnowflakeDatabase

    database = SnowflakeDatabase(
        user='user',
        password='password123',
        account='your_account',
        database='testdb',
        warehouse='your_warehouse',
    )

.. note::
    The Snowflake connector is not available on Python 3.14 due to an upstream dependency issue.

Using a different Database
--------------------------

Instead of using one of the built in database connectors, you can provide your own by extending |ClassDatabase|.


.. code-block:: python

    import vertica_python
    from pypika import VerticaQuery
    from fireant import Database

    class MyVertica(Database):
        # Vertica client that uses the vertica_python driver.

        # Override the custom PyPika Query class (Not necessary but perhaps helpful)
        query_cls = VerticaQuery

        def __init__(self, host='localhost', port=5433, database='vertica',
                     user='vertica', password=None,
                     read_timeout=None):
            self.host = host
            self.port = port
            self.database = database
            self.user = user
            self.password = password
            self.read_timeout = read_timeout

        def connect(self):
            return vertica_python.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password,
                read_timeout=self.read_timeout,
            )

        def trunc_date(self, field, interval):
            return Trunc(...)  # custom Trunc function

        def date_add(self, date_part, interval, field):
            return DateAdd(...)  # custom DateAdd function

        def convert_date(self, dt):
            return Cast(...)  # custom date conversion function. Defaults to an identity function if not provided

Once a Database connector has been set up, it can be used when instantiating |ClassDataSet|.

.. code-block:: python

    from fireant import DataSet

    my_vertica = MyVertica(
        host='example.com',
        port=5433,
        database='example',
        user='user',
        password='password123',
    )

    DataSet(
        database=my_vertica,
        ...
    )

In a custom database connector, the ``connect`` function must be overridden to provide a ``connection`` to the database.
The ``trunc_date`` and ``date_add`` functions must also be overridden since are no common ways to truncate/add dates in SQL databases.


Middleware
----------

In order to provide extra functionality as well as flexibility the database connectors allow the setup of middleware.
Default configurable middleware implementations are provided by fireant but it's also
possible to extend the middleware classes for custom functionality.

Concurrency Middleware
""""""""""""""""""""""

When executing queries on the database the operations are tunneled through a concurrency middleware. By default the
|ClassThreadPoolConcurrencyMiddleware| is used when no custom middleware is configured in the database connector.
This middleware implementation will parallelize multiple queries using a :class:`ThreadPool`.
The maximum amount of simultaneously active threads is then defined by the ``max_processes`` parameter of the database
connector.

A custom middleware can easily be created by implementing |ClassBaseConcurrencyMiddleware|. For example a
concurrency middleware that would simply execute a group of queries synchronously would look like this:

.. code-block:: python

    from fireant.middleware import BaseConcurrencyMiddleware

    class SingleThreadMiddleware(BaseConcurrencyMiddleware):
        def fetch_queries_dataframe(self, queries, database):
            return pd.concat([database.fetch_dataframe(query, database)
                              for query in queries])


.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:
