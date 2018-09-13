Connecting to the database
==========================

In order for |Brand| to connect to your database, a database connectors must be used. This takes the form of an instance of a concrete subclass of |Brand|'s ``Database`` class. Database connectors are shipped with |Brand| for all of the supported databases, but it is also possible to write your own. See below on how to extend |Brand| to support additional databases.

To configure a database, instantiate a subclass of |ClassDatabase|. You will use this instance to create a |FeatureSlicer|. It is possible to use multiple databases simultaneous, but |FeatureSlicer| can only use a single database, since they inherently model the structure of a table in the database.

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

MySQL additionally requires a custom function that |Brand| uses to rollup date values to specific intervals, equivalent to the ``TRUNC_DATE`` function available in other database platforms. To install the ``TRUNC_DATE`` function in your MySQL database, run the script found in ``fireant/scripts/mysql_functions.sql``. Further information is provided in this script on how to grant permissions on this function to your MySQL users.


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

Using a different Database
--------------------------

Instead of using one of the built in database connectors, you can provide your own by extending |ClassDatabase|.


.. code-block:: python

    import vertica_python

    class MyVertica(Database):
        # Vertica client that uses the vertica_python driver.

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

    hostage.settings = MyVertica(
        host='example.com',
        port=5433,
        database='example',
        user='user',
        password='password123',
    )

In a custom database connector, the ``connect`` function must be overridden to provide a ``connection`` to the database.
The ``trunc_date`` and ``date_add`` functions must also be overridden since are no common ways to truncate/add dates in SQL databases.


.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:
