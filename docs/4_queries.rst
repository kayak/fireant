Querying with a |FeatureSlicer|
===============================

A |FeatureSlicer| query uses a Slicer's configuration to execute a SQL query and to transform the result into a data visualization, or a widget. A single |Brand| query translates into a single SQL query, but it is possible to transform the a single query into multiple widgets. Queries are constructed with a builder pattern API.

A query object is created by calling ``slicer.data``. Subsequent function call can be chained in order to build up a query.

When fetch is called, the SQL query is executed and the resulting data set is transformed for each widget in the query. The `fetch()` method returns an array containing the data for each widget in the order that the widget was added to the query.

Example

.. code-block:: python

    from fireant.slicer import *
    from fireant.database.vertica import VerticaDatabase
    from pypika import Tables, functions as fn

    query = slicer.data \
        .widget( ... ) \
        .dimension( ... ) \
        .filter( ... ) \
        .reference( ... ) \
        .orderby( ... )

    query.fetch()

All builder methods can be called multiple times and in any order.

Builder Functions
-----------------

widget
    Add a widget to a query. A widget is what is returned from the call to ``fetch`` containing data for a visualization. At least one widget should be used, otherwise the raw data will be returned in a pandas Data Frame. See `Visualizing the data with Widgets`_ below for more details.

    The function takes one or more arguments that should be instances of subclasses of |ClassWidget|. Passing two arguments is synonymous with calling this function twice with each widget.

.. code-block:: python

    from fireant import Pandas

    slicer.data \
        ...
       .widget( Pandas(slicer.metrics.clicks, slicer.metrics.cost, slicer.metrics.revenue) )

dimension
    Add a dimension to the query. This adds a grouping level to the query that will be used to break the data down. See `Grouping data with Dimensions`_ below for more details.

    The function takes one or more arguments that must be references to dimensions of the slicer that the query is being built from.

.. code-block:: python

    slicer.data \
        ...
       .dimension( slicer.dimensions.device, slicer.dimensions.account )

filter
    Add a filter to the query. This constrains the results of the data by certain criteria. There are many types of filters, some apply to metrics and others apply to dimenions. See `Filtering the query`_ below for more details.

    The function takes one or more arguments of filter expressions using elements of the slicer that the query is being built from.

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.dimensions.date.between(date(2000, 1, 1), date(2009, 12, 31)) ) \
       .filter( slicer.dimensions.device.isin(['m', 't']) ) \
       .filter( slicer.metrics.clicks > 10 )

reference
    A reference is a way of comparing the data to itself over an interval of time using a Date/Time dimension, such as Week-over-Week. See `Comparing Data to Previous Values using References`_ below for more details.

.. code-block:: python

    from fireant import WeekOverWeek

    slicer.data \
        ...
       .reference( WeekOverWeek() )

orderby
    This function allows the results of the SQL query to be ordered by dimensions and/or metrics. Please note that this will *only* order the results of the SQL query and that the order may be affected by the Widget.
    Ordering is entirely optional. The default order will be by all of the dimensions used in the query in the order that they were added.

.. code-block:: python

    from pypika import Order

    slicer.data \
        ...
       .orderby( slicer.metrics.clicks, Order.asc )

fetch
    A call to fetch exits the build function chain and returns the results of the query. An optional hint parameter is accepted which will used in the query if monitoring the queries triggered from |Brand| fireant is needed.

.. code-block:: python

    from pypika import Order

    slicer.data \
        ...
       .fetch()


Grouping data with Dimensions
-----------------------------

Dimensions are referenced using the alias defined for them when instantiating the slicer via the ``dimensions`` attribute on the slicer instance.

.. code-block:: python

    slicer = Slicer(
        ...
        dimensions=[
            UniqueDimension('customer', ... ),
            ...
        ],
        ...
    )

    # Reference to customer dimension
    slicer.dimensions.customer

A dimension can be used in a slicer query by calling the `.dimension( ... )` method when building a query. A reference to one or more dimensions must be passed as an argument.

The order of the dimensions is important. The dimensions are grouped in the order that they are added and displayed in the widgets in that order.

.. code-block:: python

    slicer.data \
        ...
       .dimension( slicer.dimensions.device, slicer.dimensions.account )


Using intervals with Date/Time dimensions
"""""""""""""""""""""""""""""""""""""""""

All continuous dimensions require an interval to group into. For a Date/Time dimension, these intervals are common temporal intervals, such as hours, days, quarters, etc. These dimensions have a default interval and can be used without explicitly setting one. To set the interval, use the reference to the dimension as a function and pass the interval as an arguement

.. code-block:: python

    from fireant import monthly

    slicer.data \
        ...
       .dimension( slicer.dimensions.date(monthly) )

The following intervals are available for Date/Time dimensions and can be imported directly from the ``fireant`` package.

- ``hourly``
- ``daily``
- ``weekly``
- ``monthly``
- ``quarterly``
- ``annually``

It is also possible to define a custom interval as an instance of |ClassDatetimeInterval|.

Roll Up (Totals)
""""""""""""""""

Rolling up a dimension allows the totals across a dimension to be displayed in addition to the breakdown for each dimension value. To enable rollup for a dimension, call the rollup method on the dimension reference. Rollup is available for all dimension types.

.. code-block:: python

    slicer.data \
        ...
       .dimension( slicer.dimensions.date(hourly).rollup() ) \
       .dimension( slicer.dimensions.device.rollup() )

Filtering the query
-------------------

A query can be filtered using several different filters. Some filter types are used with metrics while others work with dimensions. A metric filter is synonomous with the ``HAVING`` clause in a SQL query whereas a dimension filter corresponds to the ``WHERE`` clause. Dimension filters can also be applied to the display definition of `Unique Dimensions <./3_slicer.html#unique-dimensions>`_.

When more than one filter is applied to a query, the results will be filtered to all rows/groups matching **all** of the conditions like a boolean ``AND``. Some filters accept multiple values which create multiple conditions and filter data to rows/groups matching **any** of the conditions like a boolean ``OR``.

Comparator (Metrics)
""""""""""""""""""""

Comparator filters are created using standard operators:

- `==`
- `!=`
- `>`
- `>=`
- `<`
- `<=`

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.metrics.clicks >= 100 ) \
       .filter( slicer.metrics.conversions == 1 )


Boolean (Boolean Dimensions)
""""""""""""""""""""""""""""

Boolean filters only apply to boolean dimensions and filter whether the value of that boolean dimension is `True` or False` using the `.is_( True/False )` method on a |ClassBooleanDimension|.

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.dimensions.is_member.is_(True) )

Range (Date/Time dimensions)
""""""""""""""""""""""""""""

Range filters apply to |ClassDateDimension| dimensions using the `.between( start, end )` method. This is equivalent to a `BETWEEN` expression in the SQL query.

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.dimensions.date.between( datetime(2018, 8, 21), datetime(2019, 8, 20) ) )

Includes (Category and Unique dimensions)
"""""""""""""""""""""""""""""""""""""""""

Includes filters apply to |ClassCatDimension| and |ClassUniqueDimension| dimensions using the `.isin( list )` method. Results will be included if they are equal to any of the values in the argument supplied.

Combining multiple include filters makes it possible to use both ``AND`` and ``OR`` filter logic.

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.dimensions.accounts.isin([1, 2, 3]) )

Excludes (Category and Unique dimensions)
"""""""""""""""""""""""""""""""""""""""""

Excludes filters are equivalent to Includes filters with negative logic. The same conditions apply using the `.notin( list )` method.

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.dimensions.accounts.notin([1, 2, 3]) )

Pattern (Category and Unique dimensions)
""""""""""""""""""""""""""""""""""""""""

Pattern filters apply to |ClassCatDimension| and |ClassUniqueDimension| dimensions using the `.like( *patterns )` method. They are the equivalent of a SQL ``ILIKE`` expression. The method accepts one or more pattern arguments which should be formatted for `SQL LIKE https://www.w3schools.com/sql/sql_like.asp`. With multiple arguments, results are returned that match **any** of the patterns.

Combining multiple pattern filters makes it possible to use both ``AND`` and ``OR`` filter logic.

.. code-block:: python

    slicer.data \
        ...
       .filter( slicer.dimensions.device.like('desk%', 'mob%') )

Anti-Pattern
""""""""""""

Anti-Pattern filters are equivalent to Pattern filters with negative logic. The same conditions apply using the `.not_like( *patterns )` method.

.. code-block:: python

    slicer = Slicer(
        ...
        dimensions=[
            UniqueDimension('customer',
                            definition=customers.id,
                            display_definition=fn.Concat(customers.fname, ' ', customers.lname))
        ],
        ...
    )

    slicer.data \
        ...
       .filter( slicer.dimensions.device.not_like('desk%', 'mob%') )

Filtering on Display Definitions
""""""""""""""""""""""""""""""""

When using a |ClassUniqueDimension| with the ``display_defintion`` attribute, it is also possible to filter based on display values instead of the definition.

The ``display`` attribute on an instance of |ClassUniqueDimension| returns a |ClassDisplayDimension| which works like a normal slicer dimension. It works with the following filters: `


Visualizing the data with Widgets
---------------------------------

At least one widget must be added to every query before calling the `fetch()` builder chain method. Each Slicer query can return multiple widgets of different types, but because a slicer query resolves to a single SQL query, other parts of the query must be shared across all widgets, such as filters and dimensions.

Metrics are selected for each widget. Widgets can use the same metrics or different metrics in a query. The instance API for each widget is different since each widget uses metrics in different ways.

.. code-block:: python

    slicer.data \
        ...
       .widget( ... )

matplotlib_
"""""""""""

Coming soon!

pandas_
"""""""

The Pandas widget will return a Pandas data frame which is useful when displaying results in a Jupyter_ notebook. The Pandas widget is used by instantiating a |ClassPandasWidget| class and passing one or more instances of |ClassMetric| as arguments.

The data frame will be structured with an index level for each dimension.

The Pandas widget takes additional arguments.

pivot : list[dimension]
    A list of dimensions which should be pivoted as columns. If all dimensions are pivoted, the result will be identical to setting the ``transpose`` argument to ``True``.

transpose : bool
    When ``True``, the data frame will be transposed.

sort : list[int]
    A list of column indices to sort by. This sorts the data frame after it's been pivoted and transposed. Which columns are present depends on the selected dimensions and metrics as well as the ``pivot`` and ``transponse`` arguments.

.. code-block:: python

    from fireant import Pandas

    Pandas( *metrics )

.. code-block:: python

    slicer.data \
        ...
       .dimension( slicer.dimension.date, slicer.dimension.device )
       .widget( Pandas(slicer.metrics.clicks, slicer.metrics.cost, slicer.metrics.revenue,
                       pivot=(slicer.dimension.device, )
                       transpose=True) )

HighCharts_
"""""""""""

A HighCharts widget transforms the results into a HighCharts_ JSON config object. The widget is used by instantiating |ClassHighChartsWidget| and calling the ``axis`` method with instances of |ClassHighChartsSeries| arguments. The axis method can be chained to create multiple axes.

Each |ClassHighChartsSeries| instance is constructed with one or more metrics.

.. code-block:: python

    from fireant import HighCharts

    HighCharts( title ) \
        .axis ( HighCharts.LineChart( *metrics ), HighCharts.LineChart( *metrics ), ... ) \
        .axis ( HighCharts.BarChart( *metrics ) )
        ...


Datatables_
"""""""""""

React-Table_
""""""""""""

The React Table widget's instance API is identical to the Pandas widget, although it transforms results into a JSON config object meant for React-Table_. See the section above on pandas_ for more information on the instance API.

.. code-block:: python

    from fireant import ReactTable

    slicer.data \
        ...
       .dimension( slicer.dimension.date, slicer.dimension.device )
       .widget( ReactTable(slicer.metrics.clicks, slicer.metrics.cost, slicer.metrics.revenue,
                           pivot=(slicer.dimension.device, )
                           transpose=True) )


Comparing Data to Previous Values using References
--------------------------------------------------

In some cases it is useful to compare the selected metrics over a period time such as in a Week-over-Week report. A |FeatureReference| can be used to achieve this. |FeatureReference| is a built-in function which can be chosen from the subclasses of |ClassReference|.

A |FeatureReference| can be used as a fixed comparison, a change in value (delta), or a change in value as a percentage.

The |FeatureReference| compares the currently selected data with itself shifted by the amount of the |FeatureReference|.

The following options are available

* Day Over Day - Shifts by 1 day.
* Week Over Week - Shifts by 1 week.
* Month over Month - Shifts by 1 month.
* Quarter over Quarter - Shifts by 1 quarter or 3 months depending on whether the database backend supports quarter intervals.
* Year over Year - Shifts by 1 year.

For each |FeatureReference|, there are the following variations:

* Delta - Difference in value
* Delta Percentage - Difference in value as a percentage of the previous value

A Date/Time dimension is required.

.. code-block:: python

    from fireant.slicer.references import WeekOverWeek

    # Use a Week-over-Week reference
    slicer.data \
        ...
       .reference( WeekOverWeek(slicer.dimensions.date) )

    # Compare Week-over-Week change (delta)
    slicer.data \
        ...
       .reference( WeekOverWeek(slicer.dimensions.date, delta=True) )

    # Compare Week-over-Week change as a percentage (delta percentage)
    slicer.data \
        ...
       .reference( WeekOverWeek(slicer.dimensions.date, delta=True, percent=True) )

.. note::

    For any reference, the comparison is made for the same days of the week.


Post-Processing Operations
--------------------------

Operations include extra computations applied in python to the result of the SQL query to modify the result.

More on this later!


.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:
