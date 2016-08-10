Installation and Setup
======================


.. include:: ../README.rst
   :start-after: _installation_start:
   :end-before:  _installation_end:


Once you have added |Brand| to your project, you must provide some additional settings.  A database connection is
required in order to execute queries.  Currently, only Vertica is supported via ``vertica_python``, however future plans
include support for various other databases such as MySQL and Oracle.

To configure a database, instantiate a subclass of |ClassDatabase| and set it in ``fireant.settings``.  This
must be only set once.  At the present, only one database connection is supported.

Vertica
-------

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


.. include:: ../README.rst
   :start-after: _appendix_start:
   :end-before:  _appendix_end:
