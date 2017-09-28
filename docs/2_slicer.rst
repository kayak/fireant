The Slicer
==========

.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:

The |FeatureSlicer| is the core component of |Brand| which defines a schema and an API. With a small amount of configuration, it becomes a powerful tool which can be used to quickly query data from a database and transform it into a chart, table, or other widget. The slicer can be used directly in python in Notebooks or in a python shell and provides a rich API for building quick, ad hoc queries. The |FeatureSlicer| underlies the |FeatureWidgetGroup| feature, providing a simple abstraction for building complex reports and dashboards.

Metrics
-------

Creating a |FeatureSlicer| involves extending the |ClassSlicer| class with a table, a set of |ClassMetric|, and a set of |ClassDimension|. The table identifies which table the |FeatureSlicer| will primarily be accessing, although additional tables can be joined as well to support many different possibilities. Next, a list of |ClassMetric| must be supplied which represent the data that should be accessible through the slicer. These can be defined as columns in your table as well as arithmetic expressions or aggregate SQL functions. |ClassMetric| definitions are expressed using PyPika_.


.. note::

    When defining a |ClassMetric|, it is important to note that all queries executed by fireant are aggregated over the dimensions (via a ``GROUP BY`` clause in the SQL query) and therefore are required to use aggregation functions. By default, a |ClassMetric| will use the ``SUM`` function and it's ``key``. A custom definition is commonly required  and must use a SQL aggregate function over any columns.


Dimensions
----------

Next a set of |ClassDimension| must be provided. A |ClassDimension| is a value that the data can be grouped by when aggregated. For example in a line chart, a ``TIMESTAMP`` would be commonly used as |ClassDimension| to represent the X-Axis in the chart. Subsequently, additional |ClassDimension| could be used to plot multiple curves for the same value in comparison. When rendering tables, dimensions function as the indices and with multiple |ClassDimension| can be used to create row indexes, which display a row in the table for each unique combination across all of the dimensions, or column indexes, which pivot the table all but the first dimensions to make unique columns for side-by-side comparison.

There are different types of dimensions and choosing one depends on some properties of the column.

Categorical Dimensions
""""""""""""""""""""""

A categorical dimension represents a column that contains one value from a finite set, such as a member of an enumeration.  This is the most common type of dimension.  For example, a `color` column could be used that contains values such as `red`, `green`, and `blue`.  Aggregating metrics with this dimension will give a data set grouped by each color.

Continuous Dimensions
"""""""""""""""""""""

For dimensions that do not have a finite set of values, a continuous dimension can be used.  This is especially useful for numeric values that can be viewed with varied precision, such as a decimal value.  Continuous Dimensions require an additional parameter for an ``Interval`` which groups values into discrete segments.  In a numerical example, values could be grouped by increments of 5.

Date Dimensions
"""""""""""""""

Date/Time Dimensions are a special type of continuous dimension which contain some predefined intervals: hours, days, weeks, months, quarters, and years.  In any widget that displays time series data, a date/time dimension.

Unique Dimensions
"""""""""""""""""

Lastly, a unique dimension represents a column that has one or more identifier columns and optionally a display field column.  This is useful when your data contains a significant number of values that cannot be represented by a small list of categories and is akin to using a foreign key in a SQL table.  In conjunction with a join on a foreign key, a display value can be selected from a second table and used when rendering your widgets.


.. warning::

    If the column your |FeatureDimension| uses contains ``null`` values, it is advised to define the dimension using the ``COALESE`` function in order to specify some default value.  |Brand| makes use of rollup queries that could result in collisions with null values.


.. _config_slicer_start:

Configuring a Slicer
--------------------

Here is a concrete example of a |FeatureSlicer| configuration. It includes a parameter ``metrics`` which is a list of |ClassMetric|.  Below that is the list of |ClassDimension| with a |ClassDateDimension|, |ClassCatDimension| and |ClassUniqueDimension|.

.. include:: ../README.rst
    :start-after: _slicer_example_start:
    :end-before:  _slicer_example_end:


In our example, the first couple of metrics pass ``key`` and ``label`` parameters.  The key is a unique identifier for the |FeatureSlicer| and cannot be shared by other |FeatureSlicer| elements.  The label is used as a name for metric in the component.  The last three metrics also provide a ``definition`` parameter which is a PyPika_ expression used to select the data from the database.  When a ``definition`` parameter is not supplied, the key of the metric is wrapped in a ``Sum`` function as a default.  The metric for ``impressions`` will get the definition ``fn.Sum(analytics.impressions)``.

Here a few dimensions as also defined.  A |ClassDateDimension| is used with a custom definition which maps to the ``dt`` column in the database.  The Device dimension uses the column with the same name as the key ``device`` as a default. There are three possible values for a device: 'desktop', 'tablet', or 'mobile', so a  |ClassCatDimension| is a good fit. Last there is a  |ClassUniqueDimension| which uses the column ``account_id`` as an identifier but the column ``account_name`` as a display field for each account value.  Both columns will be included in the query.

.. _config_slicer_end:

Columns from Joined Tables
""""""""""""""""""""""""""

Commonly data from a secondary table is required.  These tables can be joined in the query so that their columns become available for use in metric and dimension definitions.  Joins must be defined in the slicer in order to use them, and metrics and dimensions must also define which joins they require, so that they can be added to the query when used.

A join requires three parameters, a *key*, a *table*, and a *criterion*.  The *key* is used for reference when using the join on a metric or dimension, the *table* is the table which is being joined.  The *criterion* is a PyPika_ expression which defines how to join the tables, more concretely an equality condition of when to join rows of each table.

.. code-block:: python


    from fireant.slicer import *
    from fireant.database.vertica import VerticaDatabase
    from pypika import Tables, functions as fn

    vertica_database = VerticaDatabase(user='fakeuser', password='fakepassword')
    analytics, customers = Tables('analytics', 'customers')

    slicer = Slicer(
        table=analytics,
        database=vertica_database,

        joins=[
            Join('customers', customers, analytics.customer_id == customers.id),
        ],

        metrics=[
            Metric('clicks', 'Clicks'),
        ],

        dimension=[
            UniqueDimension('customer', definition=customers.id,
                            display_field=fn.Concat(customers.fname, ' ', customers.lname),
                            joins=['customers'])
        ],
    )



Slicer and Transformer Managers
-------------------------------

The |FeatureSlicer| expose different managers for different types of request.  The primary one is the Slicer manager which exposes a ``data`` function which returns the query results as Pandas_ data frame.  Transformer managers provide the additional functionality of converting your data into a specified format.  There are several transformers available by default as well as optional ones which require additional python dependencies.

The ``notebooks`` transformer manager is the default one which is intended for use in Jupyter_ notebookss.  In this tutorial it will be used exclusively.  All transformer managers expose different methods for different types of results, but the methods always have the same signature.

.. include:: ../README.rst
    :start-after: _manager_api_start:
    :end-before:  _manager_api_end:


Getting Raw Data
----------------

Now it is possible to put all the pieces together and start using the |FeatureSlicer|. Each of the manager functions has the following signature containing ``tuple`` or ``list`` of |ClassMetric|, |ClassDimension|.  The other parameters will be introduced in a later section.  For now, only metrics and dimensions are required to start fetching data.

The ``metrics`` parameter is always a list of ``str`` matching the ``key`` of the desired metrics defined in the slicer. The ``dimensions`` parameter is a list of mixed types but most often a ``str`` referencing the keys of the desired dimensions.  Continuous dimensions can also optionally specify an interval.  DateDimensions by default use the interval ``DatetimeDimension.day``.


When calling a |ClassSlicerManager| function, the ``tuple`` of metrics should contain string values matching the ``name`` of a |ClassMetric| or |ClassDimension| selected in the configuration.


Highcharts Line Charts
----------------------

Below is an example of how to request a Highcharts_ line chart from the |FeatureSlicer|.  The return value will be a ``dict`` which can be serialized into a JSON and used directly by Highcharts_.  The example chart includes six lines: Clicks (Desktop), Clicks (Tablet), Clicks (Mobile) and Conversions (Desktop), Conversions (Tablet), Conversions (Mobile).

The ``metrics`` and ``dimensions`` parameters are a ``list`` or ``tuple`` of ``str`` matching the key of an element configured in the |FeatureSlicer|.

.. note::

    Line Charts *require* a |ClassContDimension| or a |ClassDateDimension| as the first selected dimension since it is used as the X-Axis.  Subsequent dimensions can be used and will be split into different lines in the chart for comparisons across a dimension.

.. code-block:: python

    result = slicer.highcharts.line_chart(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device_type']
    )


When using a |ClassDateDimension|, the default interval is a day.  To change the interval, pass a ``tuple`` instead of a ``string`` as a parameter with the first element matching the metric key in the |FeatureSlicer| and the second element as a ``DatetimeDimension``.

.. code-block:: python

    result = slicer.highcharts.line_chart(
        metrics=['clicks', 'conversions'],
        dimensions=[('date', DatetimeDimension.year), 'device_type']
    )


The result can then be serialized to JSON:

.. code-block:: python

    import json

    json.dumps(result)


Highcharts Column and Bar Charts
--------------------------------

Column charts and bar charts are also available in the Highcharts_ transformer.  The output format is the same, except for the ``chart_type`` option.

.. note::

    Column and Bar charts require *one* or *two* dimensions, preferably of type |ClassCatDimension|, but this is not required.

.. code-block:: python

    slicer.highcharts.bar_chart(
        metrics=['clicks', 'conversions'],
        dimensions=[('date', DatetimeDimension.year), 'device_type']
    )

.. code-block:: python

    slicer.highcharts.column_chart(
        metrics=['clicks', 'conversions'],
        dimensions=['device_type'],
    )

Tables
------

Tables don't have any requirements as to the number or types of dimensions and generally can display any type of |FeatureSlicer| result.  The return value is given in Datatables_ format.

In a *row-indexed* Table, the rows of the table each will have a unique combination of values of the dimensions.  Below is an example that will give the following columns: Day, Device Type, Clicks, Conversions.

.. code-block:: python

    slicer.datatables.row_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device_type']
    )


A *column-indexed* table will contain only one index column and display a metrics column for each combination of subsequent dimensions.  For example with the same parameters as above, the result will include seven columns: Day, Clicks (Desktop), Clicks (Tablet), Clicks (Mobile) and Conversions (Desktop), Conversions (Tablet), and Conversions (Mobile).

.. code-block:: python

    slicer.datatables.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device_type']
    )


.. note::

    *Column-indexed* tables use the setting ``datatables_maxcols`` to avoid creating uncontrollably large tables.

Filtering Data
--------------

So far all of the examples will display a result that queries all of the data in the database table. In many cases it is also useful to narrow the window of data to display in the result.  Filters are used for this purpose.  Filters are expressions that refer to a defined metric or dimension and some criteria.  For example, it might be desirable to only display data for `desktop` devices.

.. note::

    Filters can be used for either metrics or dimensions.  Filtering metrics is synonymous to the ``HAVING`` clause in SQL whereas dimensions is synoymous with the ``WHERE`` clause.

Equality and Inequality Filters
"""""""""""""""""""""""""""""""

The most basic type of filtering uses a equality/inequality expression such as `a=b` or `a>b`.

.. code-block:: python

    from fireant.slicer import EqualityFilter, EqualityOperator

    # Only desktop data
    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[EqualityFilter('device_type', EqualityOperator.eq, 'desktop')],
    )

.. code-block:: python

    from fireant.slicer import EqualityFilter, EqualityOperator

    # Only data for days where clicks were greater than 100
    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        metric_filters=[EqualityFilter('clicks', EqualityOperator.gt, 100)],
    )


Multiple Choice Filters
"""""""""""""""""""""""

When a column should be equal to one of a set of values, a `Contains` filter can be used.

.. code-block:: python

    from fireant.slicer import ContainsFilter

    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[ContainsFilter('device_type', ['desktop', 'mobile'])],
    )


Range Filters
"""""""""""""

`Range` filters are used to restrict the query to a window of values.

.. code-block:: python

    from fireant.slicer import RangeFilter

    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[RangeFilter('date', date.today() - timedelta(days=60), date.today())],
    )

Wildcard Filters
""""""""""""""""

For pattern matching a `Fuzzy` can be used which parallels ``LIKE`` expressions in SQL.

.. code-block:: python

    from fireant.slicer import WildcardFilter

    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[WildcardFilter('device_type', 'tab%')],
    )

Filter Unique Dimensions
""""""""""""""""""""""""

Unique dimensions are a special case since they have both a definition field as well as a display field.  Filtering can be performed on either of these fields.  The default behavior filters using the definition, as in other dimensions.  To filter using the display_field, pass a ``tuple`` like ``(key, "display")`` for the ``element_key`` parameter.

.. code-block:: python

    from fireant.slicer import EqualityFilter, WildcardFilter

    # Filters by account ID
    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[EqualityFilter('account', 'eq', 42)],
    )

    # Filters by account Name
    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[WildcardFilter(('account', 'display'), 'abc%')],
    )


Comparing Data to Previous Values
---------------------------------

In some cases it is useful to compare current numbers to previous values such as in a Week-over-Week report.  A |FeatureReference| can be used to achieve this. |FeatureReference| is a built-in function which can be chosen from the subclasses of |ClassReference|.

A |FeatureReference| can be used as a fixed comparison, a change in value (delta), or a change in value as a percentage.

The following options are available

* Week Over Week - Compares a day with the day 1 week prior
* Month over Month - Compares a day with the day 4 weeks prior
* Quarter over Quarter - Compares a day with the day 12 weeks prior
* Year over Year - Compares a day with the day 52 weeks prior

For each |FeatureReference|, there are the following variations:

* Delta - Difference in value
* Delta Percentage - Difference in value as a percentage of the previous value

.. code-block:: python

    from fireant.slicer.references import WoW, MoM, QoQ, Delta, DeltaPercentage

    slicer.notebooks.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        references=[WoW('date'), Delta(MoM('date')), DeltaPercentage(QoQ('date'))],
    )

.. note::

    For any reference, the comparison is made for the same days of the week.


Post-Processing Operations
--------------------------

Operations include extra computations that modify the final results.

Totals
""""""

Totals adds ``ROLLUP`` to the SQL query to load the data and aggregated across dimensions.  It requires one or more dimension keys as parameters for the dimensions that should be totaled.  The below example will add an extra line with the total clicks and conversions for each date in addition to the three lines for each device type, desktop, mobile and tablet.

.. note::

    Please note, the ROLLUP functionality is not currently supported for MySQL, PostgreSQL and Amazon Redshift as they either implement ROLLUP differently to other Database platforms, or do not support ROLLUP operations at all. An exception will be raised if you try and use this.

.. code-block:: python

    from fireant.slicer.operations import Totals

    slicer.highcharts.line_chart(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device'],
        operations=[Totals('device')],
    )

L1 and L2 Loss
""""""""""""""

Coming soon

Pagination
----------
FireAnt also exposes an API to enable data pagination. The ``fireant.slicer.pagination.Paginator`` object
can be used to define a query limit, offset and order bys.

.. code-block:: python

    from pypika import Order

    slicer.datatables.row_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device'],
        pagination=Paginator(offset=10, limit=10, order=[('clicks', Order.desc), ('conversions', Order.asc))])
    )

.. note::

    Please note, you cannot use operations with paginated tables. This is because operations are applied to the data once it has
    been retrieved from the database, so the operation value would be reset on each page once new data has been retrieved.