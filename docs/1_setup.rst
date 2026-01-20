Installation and Setup
======================

.. include:: ../README.rst
   :start-after: _installation_start:
   :end-before:  _installation_end:


Database Connector add-ons
--------------------------

By default, |Brand| does not include any database drivers. You can optionally include one or provide your own. The following extra installations will set up |Brand| with the recommended drivers. |Brand| may work with additional drivrers, but is untested and requires a custom extension of the |ClassDatabase| class (see below).


.. code-block:: bash

    # Vertica
    pip install fireant[vertica]

    # MySQL
    pip install fireant[mysql]

    # PostgreSQL
    pip install fireant[postgresql]

    # Amazon Redshift
    pip install fireant[redshift]

    # Snowflake
    pip install fireant[snowflake]

    # Microsoft SQL Server (requires FreeTDS)
    pip install fireant[mssql]

.. note::
    The Snowflake extra is not available on Python 3.14 due to an upstream dependency issue
    (``snowflake-connector-python`` requires ``cffi<2``, which lacks Python 3.14 support).

Transformer add-ons
-------------------

Some transformers have additional dependencies that are not included by default. Include the following extra installations in your `requirements.txt` file if you intend to use those transformers.

.. code-block:: bash

    # matplotlib
    pip install fireant[matplotlib]


.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:
