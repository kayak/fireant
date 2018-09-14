Creating a |FeatureSlicer|
==========================

A |FeatureSlicer| consists of five main parts: A database connector, a primary database table, join tables, dimensions, and metrics. The database connector is a connection to a database where the primary and join tables exist. Both dimensions and metrics are given definitions using PyPika_ expressions which are effectively converted into SQL when a query is executed. Metrics are quantifiers that are aggregated across Dimensions. Dimensions and metrics can be thought of as ``SELECT`` and ``GROUP BY`` clauses in a SQL query, concretely that is how they are used.

Once a |FeatureSlicer| is configured, it can be used to write queries using a builder pattern. A query selects which dimensions and metrics to use and how to transform that data into a visualization such as a chart or table. Queries also provide a mechanism to filter on dimension and metric values, which is equivalent to the ``WHERE`` and ``HAVING`` clauses of the SQL query.

The key assumption in using a |FeatureSlicer| is that when analyzing data, definitions for dimensions or metrics do not change often, but which combinations of them are used and how the data is visualized does. Therefore, the |FeatureSlicer| removes much of the boilerplate in building visualizations and dashboards.

The slicer class
----------------

Creating a |FeatureSlicer| involves instantiating a |ClassSlicer| with at a minimum a database, primary table, and one metric.

.. code-block:: python

    from fireant.slicer import *
    from fireant.database.vertica import VerticaDatabase
    from pypika import Tables, functions as fn

    vertica_database = VerticaDatabase(user='jane_doe', password='strongpassword123')
    analytics, customers = Tables('analytics', 'customers')

    slicer = Slicer(
        database=vertica_database,
        table=analytics,

        joins=[
            Join(customers, analytics.customer_id == customers.id),
        ],

        dimensions=[
            UniqueDimension('customer',
                            definition=customers.id,
                            display_field=fn.Concat(customers.fname, ' ', customers.lname))
        ],

        metrics=[
            Metric('clicks', 'Clicks'),
        ],
    )

Metrics
-------

A |FeatureMetric| is a quantifier which is aggregated across dimensions when used in a |FeatureSlicer| query. A |FeatureSlicer| requires at a minimum one metric. Metrics are the values used to draw lines or bars in charts or fill cells in tables, the measurements in the data.

A |FeatureMetric| is represented in code by the class |ClassMetric|. When instantiating a |FeatureSlicer|, at least one instance of |ClassMetric| must be given in the ``metrics`` argument.

.. code-block:: python

    from pypika import Table
    from fireant import Metric

    analytics = Table('analytics')

    roi = Metric('roi',
                 label='Return on Investment',
                 definition=fn.Sum(analytics.revenue) / fn.Sum(analytics.cost),
                 precision=3,
                 suffix='%'),

.. note::

    When defining a |ClassMetric|, it is important to note that all queries executed by fireant are aggregated over the dimensions (via a ``GROUP BY`` clause in the SQL query) and therefore are required to use aggregation functions. By default, a |ClassMetric| will use the ``SUM`` function and it's ``key``. A custom definition is commonly required  and must use a SQL aggregate function over any columns.

Dimensions
----------

There are different types of dimensions and choosing one depends on some properties of the column.

Continuous Dimensions
"""""""""""""""""""""

For dimensions that do not have a finite set of values, a continuous dimension can be used. This is especially useful for numeric values that can be viewed with varied precision, such as a decimal value. Continuous Dimensions require an additional parameter for an ``Interval`` which groups values into discrete segments. In a numerical example, values could be grouped by increments of 5.

Date/Time Dimensions
""""""""""""""""""""

Date/Time Dimensions are a special type of continuous dimension which contain some predefined intervals: hours, days, weeks, months, quarters, and years. In any widget that displays time series data, a date/time dimension.

.. code-block:: python

    from fireant import DatetimeDimension

    DatetimeDimension('timestamp',
                      label='Timestamp',
                      definition=customers.ts)

Boolean Dimensions
""""""""""""""""""

A boolean dimension is a dimension that represents a boolean field.

.. code-block:: python

    from fireant import BooleanDimension

    BooleanDimension('is_active',
                     label='Active',
                     definition=customers.active)

Categorical Dimensions
""""""""""""""""""""""

A categorical dimension represents a column that contains one value from a finite set, such as a member of an enumeration. This is the most common type of dimension. For example, a `color` column could be used that contains values such as `red`, `green`, and `blue`. Aggregating metrics with this dimension will give a data set grouped by each color.

.. code-block:: python

    from fireant import CategoricalDimension

    CategoricalDimension('device',
                         label='Device Type',
                         definition=customers.device
                         display_values=(
                            ('d', 'Desktop'),
                            ('m', 'Mobile'),
                            ('t', 'Tablet'),
                         ))

Unique Dimensions
"""""""""""""""""

A unique dimension represents a column that has one or more identifier columns and optionally a display field column. This is useful when your data contains a significant number of values that cannot be represented by a small list of categories and is akin to using a foreign key in a SQL table. In conjunction with a join on a foreign key, a display value can be selected from a second table and used when rendering your widgets.

.. code-block:: python

    from fireant import UniqueDimension

    UniqueDimension('customer',
                    label='Customer',
                    definition=customers.id,
                    display_field=fn.Concat(customers.fname, ' ', customers.lname))


Joins
-----

A |FeatureSlicer| can be configured with additional join tables that will automatically be when using a metric or dimension that requires it. |Brand| determines that the join is required if the joined table defined in the join is used in the definition of a dimension or metric. Joins can also join from other joins.

A join is defined with two arguments: the join table and a conditional expression of how to join the table. The join expression should be used as a condition in the ``ON`` clause of a join in a SQL query.

.. code-block:: python

    from fireant import Join

    Join(customers, analytics.customer_id == customers.id)
    Join(orders, (orders.customer_id == customers.id) & (orders.store_id == store.id))


.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:
