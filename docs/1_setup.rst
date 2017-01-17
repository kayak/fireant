Installation and Setup
======================

.. include:: ../README.rst
   :start-after: _installation_start:
   :end-before:  _installation_end:


Database Connector add-ons
--------------------------


By default, |Brand| does not include any database drivers.  You can optionally include one or provide your own.

Vertica

.. code-block:: bash

    pip install fireant[vertica]

Transformer add-ons
-------------------

There are also optional transformer packages which give access to different widget libraries.  This only applies to transformers that require additional packages.  All other transformers are included by default.


matplotlib

.. code-block:: bash

    pip install fireant[matplotlib]


Once you have added |Brand| to your project, you must provide some additional settings.  A database connection is required in order to execute queries.  Currently, only Vertica is supported via ``vertica_python``, however future plans include support for various other databases such as MySQL and Oracle.

To configure a database, instantiate a subclass of |ClassDatabase| and set it in ``fireant.settings``.  This must be only set once.


.. code-block:: python

    import fireant.settings
    from fireant.database import Vertica

    fireant.settings = Vertica(
        host='example.com',
        port=5433,
        database='example',
        user='user',
        password='password123',
    )


Custom Database
---------------

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

        def round_date(self, field, interval):
            return Round(field, interval)

    hostage.settings = MyVertica(
        host='example.com',
        port=5433,
        database='example',
        user='user',
        password='password123',
    )

In a custom database connector, the ``connect`` function must be overridden to provide a ``connection`` to the database. The ``round_date`` function must also be overridden since there is no common way to round dates in SQL databases.



.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end: