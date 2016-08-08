The Slicer
==========

.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:

The |FeatureSlicer| is the core component of |Brand| which defines a schema and an API. With a small amount of
configuration, it becomes a powerful tool which can be used to quickly query data from a database and transform it into
a chart, table, or other widget. The slicer can be used directly in python in Notebooks or in a python shell and
provides a rich API for building quick, ad hoc queries. The |FeatureSlicer| underlies the |FeatureWidgetGroup| feature,
providing a simple abstraction for building complex reports and dashboards.

Metrics
-------

Creating a |FeatureSlicer| involves extending the |ClassSlicer| class with a table, a set of |ClassMetric|, and
a set of |ClassDimension|. The table identifies which table the |FeatureSlicer| will primarily be accessing,
although additional tables can be joined as well to support many different possibilities. Next, a list of
|ClassMetric| must be supplied which represent the data that should be accessible through the slicer. These can be
defined as columns in your table as well as arithmetic expressions or aggregate SQL functions. |ClassMetric|
definitions are expressed using PyPika_.


.. note::

    When defining a |ClassMetric|, it is important to note that all queries executed by hostage are aggregated over
    the dimensions (via a ``GROUP BY`` clause in the SQL query) and therefore are required to use aggregation functions.
    By default, a |ClassMetric| will use the ``SUM`` function and it's ``key``. A custom definition is commonly required
    and must use a SQL aggregate function over any columns.


Dimensions
----------

Next a set of |ClassDimension| must be provided. A |ClassDimension| is a value that the data can be grouped by when
aggregated. For example in a line chart, a ``TIMESTAMP`` would be commonly used as |ClassDimension| to represent the
X-Axis in the chart. Subsequently, additional |ClassDimension| could be used to plot multiple curves for the same value
in comparison. When rendering tables, dimensions function as the indices and with multiple |ClassDimension| can be
used to create row indexes, which display a row in the table for each unique combination across all of the dimensions,
or column indexes, which pivot the table all but the first dimensions to make unique columns for side-by-side
comparison.

There are different types of dimensions and choosing one depends on some properties of the column.

Categorical Dimensions
""""""""""""""""""""""

A categorical dimension represents a column that contains one value from a finite set, such as a member of an
enumeration.  This is the most common type of dimension.  For example, a `color` column could be used that contains
values such as `red`, `green`, and `blue`.  Aggregating metrics with this dimension will give a data set grouped by
each color.

Continuous Dimensions
"""""""""""""""""""""

For dimensions that do not have a finite set of values, a continuous dimension can be used.  This is especially useful
for numeric values that can be viewed with varied precision, such as a decimal value.  Continuous Dimensions require
an additional parameter for an ``Interval`` which groups values into discrete segments.  In a numerical example, values
could be grouped by increments of 5.

Date Dimensions
"""""""""""""""

Date/Time Dimensions are a special type of continuous dimension which contain some predefined intervals: hours, days,
weeks, months, quarters, and years.  In any widget that displays time series data, a date/time dimension.

Unique Dimensions
"""""""""""""""""

Lastly, a unique dimension represents a column that has one or more identifier columns and optionally a display label
column.  This is useful when your data contains a significant number of values that cannot be represented by a small
list of categories and is akin to using a foreign key in a SQL table.  In conjunction with a join on a foreign key, a
display value can be selected from a second table and used when rendering your widgets.


.. warning::

    If the column your |FeatureDimension| uses contains ``null`` values, it is advised to define the dimension using the
    ``COALESE`` function in order to specify some label for that value.  |Brand| makes use of advanced queries that
    could lead to collisions with null values.


.. _config_slicer_start:

Configuring a Slicer
--------------------

Here is a concrete example of a |FeatureSlicer| configuration. It includes a parameter ``metrics`` which is a list of
|ClassMetric|.  Below that is the list of |ClassDimension| with a |ClassDateDimension|, |ClassCatDimension| and
|ClassUniqueDimension|.

.. code-block:: python

    from hostage.slicer import *
    from pypika import Table, functions as fn

    analytics = Table('analytics')

    slicer = Slicer(
        analytics,

        metrics=[
            Metric("impressions", "Impressions"),
            Metric('clicks', 'Clicks'),
            Metric("conversions", "Conversions"),
            Metric("cost", "Cost"),
            Metric("revenue", "Revenue"),

            Metric("cpc", "CPC",
                   definition=fn.Sum(analytics.cost) / fn.Sum(analytics.clicks)),
            Metric("rpc", "RPC",
                   definition=fn.Sum(analytics.revenue) / fn.Sum(analytics.clicks)),
            Metric("roi", "ROI",
                   definition=fn.Sum(analytics.revenue) / fn.Sum(analytics.cost)),
        ],

        dimensions=[
            DatetimeDimension('date', 'Date', definition=analytics.dt),
            CategoricalDimension('device', 'Device'),
            UniqueDimension('account',
                            label='Account',
                            id_fields=[analytics.account_id],
                            label_field=analytics.account_name),
        ],
    )


In our example, the first couple of metrics pass ``key`` and ``label`` parameters.  The key is a unique identifier for
the |FeatureSlicer| and cannot be shared by other |FeatureSlicer| elements.  The label is used when transforming the
data into widgets to represent the field.  The last three metrics also provide a ``definition`` parameter which is a
PyPika_ expression used to select the data from the database.  When a ``definition`` parameter is not supplied, the key
of the metric is wrapped in a ``Sum`` function as a default.  The metric for ``impressions`` will get the definition
``fn.Sum(analytics.impressions)``.

Here a few dimensions as also defined.  A |ClassDateDimension| is used with a custom definition which maps to the ``dt``
column in the database.  The Device dimension uses the column with the same name as the key ``device`` as a default.
There are three possible values for a device: 'desktop', 'tablet', or 'mobile', so a  |ClassCatDimension| is a good fit.
Last there is a  |ClassUniqueDimension| which uses the column ``account_id`` as an identifier but the column
``account_name`` as a display label.  Both columns will be included in the query.

.. _config_slicer_end:


Using the Slicer Manager
------------------------

After defining a |FeatureSlicer|, you are reading to query data into charts, tables, and widgets.  Each of these
components is created via a |FeatureTransformer|.  A |FeatureTransformer| is a thin layer in between the raw data and
the presentation.  |Brand| includes a suite of prebuilt transformers but it is also possible to create your own for
custom output formats.  When requesting data, a transformer must be selected.  Requests can also include multiple
transformers but that will be covered in a later section.

The slicer contains a manager class, |ClassSlicerManager| which offers a method for each transformer and a ``data`` method
which foregoes transformation and returns a Pandas_ data frame.  The default transformers included in |Brand| include
the following:

* ``slicer.manager.data`` - A Pandas_ dataframe indexed by the selected dimensions.
* ``slicer.manager.line_chart`` - A Highcharts_ line chart.
* ``slicer.manager.bar_chart`` - A Highcharts_ bar chart.
* ``slicer.manager.row_index_table`` - A Datatables_ row-indexed table.
* ``slicer.manager.column_index_table`` - A Datatables_ column-indexed table.


Getting Raw Data
----------------

Now it is possible to put all the pieces together and start using the |FeatureSlicer|. Each of the manager functions has
the following signature containing ``tuple``s or ``list``s of |ClassMetric|, |ClassDimension|.  The other parameters
will be introduced in a later section.  For now, only metrics and dimensions are required to start fetching data.

The ``metrics`` parameter is always a list of ``str`` matching the ``key`` of the desired metrics defined in the slicer.
The ``dimensions`` parameter is a list of mixed types but most often a ``str`` referencing the keys of the desired
dimensions.  Continuous dimensions can also optionally specify an interval.  DateDimensions by default use the
interval ``DatetimeDimension.day``.

.. code-block:: python

    def data(self, metrics, dimensions, metric_filters, filters, references, operations):
        pass


When calling a |ClassSlicerManager| function, the ``tuple`` of metrics should contain string values matching the
``name`` of a |ClassMetric| or |ClassDimension| selected in the configuration.


Line Charts
-----------

Below is an example of how to request a Highcharts_ line chart from the |FeatureSlicer|.  The return value will be a
``dict`` which can be transformed into a JSON and used directly by Highcharts_.  The example chart includes six lines:
Clicks (Desktop), Clicks (Tablet), Clicks (Mobile) and Conversions (Desktop), Conversions (Tablet), Conversions
(Mobile).

The ``metrics`` and ``dimensions`` parameters are a ``list`` or ``tuple`` of ``str`` matching the key of an element
configured in the |FeatureSlicer|.

.. note::

    Line Charts *require* a |ClassContDimension| or a |ClassDateDimension| as the first selected dimension since it is
    used as the X-Axis.  Subsequent dimensions can be used and will be split into different lines in the chart for
    comparisons across a dimension.

.. code-block:: python

    slicer.manager.line_chart(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device_type']
    )


When using a |ClassDateDimension|, the default interval is a day.  To change the interval, pass a ``tuple`` instead of
a ``string`` as a parameter with the first element matching the metric key in the |FeatureSlicer| and the second element
as a ``DatetimeDimension``.

.. code-block:: python

    slicer.manager.line_chart(
        metrics=['clicks', 'conversions'],
        dimensions=[('date', DatetimeDimension.year), 'device_type']
    )


Column and Bar Charts
---------------------

Similar to Line Charts, column and bar charts are also given in the Highcharts_ format.  The return value from the
|FeatureSlicer| is practically the same for the two, only they are rendered in slightly different formats.  Namely,
Bar charts are oriented horizontally and Column charts vertically.

.. note::

    Column and Bar charts require *one* or *two* dimensions, preferably of type |ClassCatDimension|, but this is not
    required.

.. code-block:: python

    slicer.manager.bar_chart(
        metrics=['clicks', 'conversions'],
        dimensions=[('date', DatetimeDimension.year), 'device_type']
    )

.. code-block:: python

    slicer.manager.column_chart(
        metrics=['clicks', 'conversions'],
        dimensions=['device_type'],
    )

Tables
------

Tables don't have any requirements as to the number or types of dimensions and generally can display any type of
|FeatureSlicer| result.  The return value is given in Datatables_ format.

In a *row-indexed* Table, the rows of the table each will have a unique combination of values of the dimensions.  Below
is an example that will give the following columns: Day, Device Type, Clicks, Conversions.

.. code-block:: python

    slicer.manager.row_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device_type']
    )


A *column-indexed* table will contain only one index column and display a metrics column for each combination of
subsequent dimensions.  For example with the same parameters as above, the result will include seven columns: Day,
Clicks (Desktop), Clicks (Tablet), Clicks (Mobile) and Conversions (Desktop), Conversions (Tablet), and Conversions
(Mobile).

.. code-block:: python

    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date', 'device_type']
    )


Filtering Data
--------------

So far all of the examples will display a result that queries all of the data in the database table. In many cases it is
also useful to narrow the window of data to display in the result.  Filters are used for this purpose.  Filters are
expressions that refer to a defined metric or dimension and some criteria.  For example, it might be desirable to only
display data for `desktop` devices.

.. note::

    Filters can be used for either metrics or dimensions.  Filtering metrics is synonymous to the ``HAVING`` clause in
    SQL whereas dimensions is synoymous with the ``WHERE`` clause.

Equalities and Inequalities
"""""""""""""""""""""""""""

The most basic type of filtering uses a equality/inequality expression such as `a=b` or `a>b`.

.. code-block:: python

    from hostage.slicer import EqualityFilter, EqualityOperator

    # Only desktop data
    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[EqualityFilter('device_type', EqualityOperator.eq, 'desktop')],
    )

.. code-block:: python

    from hostage.slicer import EqualityFilter, EqualityOperator

    # Only data for days where clicks were greater than 100
    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        metric_filters=[EqualityFilter('clicks', EqualityOperator.gt, 100)],
    )


Multiple Choice
"""""""""""""""

When a column should be equal to one of a set of values, a `Contains` filter can be used.

.. code-block:: python

    from hostage.slicer import ContainsFilter

    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[ContainsFilter('device_type', ['desktop', 'mobile'])],
    )


Windows
"""""""

`Range` filters are used to restrict the query to a window of values.

.. code-block:: python

    from hostage.slicer import RangeFilter

    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[RangeFilter('date', date.today(), date.today() - timedelta(days=60))],
    )

Wildcard Matching
"""""""""""""""""

For pattern matching a `Fuzzy` can be used which parallels ``LIKE`` expressions in SQL.

.. code-block:: python

    from hostage.slicer import WildcardFilter

    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        dimension_filters=[WildcardFilter('account', 'abc%')],
    )


Comparing Data to Previous Values
---------------------------------

In some cases it is useful to compare current numbers to previous values such as in a Week-over-Week report.  A
|FeatureReference| can be used to achieve this. |FeatureReference| is a built-in function which can be chosen from the
subclasses of |ClassReference|.

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

    from hostage.slicer import WoW, DeltaMoM, DeltaQoQ

    slicer.manager.column_index_table(
        metrics=['clicks', 'conversions'],
        dimensions=['date'],
        references=[WoW('date'), DeltaMoM('date'), DeltaQoQ('date')],
    )

.. note::

    For any reference, the comparison is made for the same days of the week.