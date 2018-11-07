FireAnt - Analytics and Reporting
=================================

.. _intro_start:

|BuildStatus|  |CoverageStatus|  |Codacy|  |Docs|  |PyPi|  |License|


|Brand| is a a data analysis tool used for quickly building charts, tables, reports, and dashboards. It defines a schema for configuring metrics and dimensions which removes most of the leg work of writing queries and formatting charts. |Brand| even works great with Jupyter notebooks and in the Python shell providing quick and easy access to your data.

.. _intro_end:

Read more at http://fireant.readthedocs.io/en/latest/

Installation
------------

.. _installation_start:

To install |Brand|, run the following command in the terminal:

.. code-block:: bash

    pip install fireant


.. _installation_end:

Introduction
------------

|Brand| arose out of an environment where several different teams, each working with data sets often with crossover, were individually building their own dashboard platforms. |Brand| was developed as a centralized way of building dashboards without the legwork.

|Brand| is used to create configurations of data sets using |FeatureSlicer| which backs a database table containing analytics and defines sets of |FeatureDimension| and |FeatureMetric|. A |FeatureDimension| is used to group data by properties, such as a timestamp, an account, a device type, etc. A |FeatureMetric| is used to render quanitifiers such as clicks, ROI, conversions into a widget such as a chart or table.

A |FeatureSlicer| exposes a rich builder API that allows a wide range of queries to be constructed that can be rendered as several widgets. A |FeatureSlicer| can be used directly in a Jupyter_ notebook, eliminating the need to write repetitive custom queries and render the data in visualizations.

Slicers
-------

|FeatureSlicer| are the core component of |Brand|. A |FeatureSlicer| is a representation of a data set and is used to execute queries and transform result sets into widgets such as charts or tables.

A |FeatureSlicer| requires only a couple of definitions in order to use: A database connector, a database table, join tables, and dimensions and metrics. Metrics and Dimension definitions tell |Brand| how to query and use data in widgets. Once a slicer is created, it's query API can be used to build queries with just a few lines of code selecting which dimensions and metrics to use and how to filter the data.

.. _slicer_example_start:

Instantiating a Slicer
""""""""""""""""""""""

.. code-block:: python

    from fireant.slicer import *
    from fireant.database import VerticaDatabase
    from pypika import Tables, functions as fn

    vertica_database = VerticaDatabase(user='myuser', password='mypassword')
    analytics, accounts = Tables('analytics', 'accounts')

    my_slicer = Slicer(
        # This is the primary database table that our slicer uses
        table=analytics,

        # Define the database connection object
        database=vertica_database,

        joins=[
            # Metrics and dimensions can use columns from joined tables by
            # configuring the join here. Joins will only be used when necessary.
            Join('accounts', accounts, analytics.account_id == accounts.id),
        ],

        metrics=[
            # A unique key is required for each metric
            Metric('impressions'),
            Metric('clicks'),
            Metric('conversions'),
            Metric('cost'),
            Metric('revenue'),

            # By default, a metric maps one-to-one with a column in the database
            # but it can also be given a more complex definition.
            Metric('cpc', label='CPC',
                   definition=fn.Sum(analytics.cost) / fn.Sum(analytics.clicks)),
            Metric('rpc', label='RPC',
                   definition=fn.Sum(analytics.revenue) / fn.Sum(analytics.clicks)),
            Metric('roi', label='ROI',
                   definition=fn.Sum(analytics.revenue) / fn.Sum(analytics.cost)),
        ],

        dimensions=[
            # Datetime Dimensions are continuous and must be truncated to an interval
            # like hour, day, week. Day is the default.
            DatetimeDimension('date', definition=analytics.dt),

            # Categorical dimensions are ones with a fixed number of options.
            CategoricalDimension('device', display_options=[DimensionValue('desktop'),
                                                    DimensionValue('tablet'),
                                                    DimensionValue('mobile')]),

            # Unique dimensions are used for entities that have a unique ID and
            # a display name field
            UniqueDimension('account', label='Account Name', definition=analytics.account_id,

                            # The accounts table is joined to get more data about the
                            # account.
                            display_field=accounts.name,

                            # Just a list of keys of the required joins is needed.
                            joins=['accounts']),
        ],
    )

.. _slicer_example_end:

.. _slicer_query_example_start:

Building queries with a Slicer
""""""""""""""""""""""""""""""

Use the ``data`` attribute start building a slicer query. A slicer query allows method calls to be chained together to select what should be included in the result.

This example uses the slicer defined above

.. code-block:: python

   from fireant import Matplotlib, Pandas, daily

    matplotlib_chart, pandas_df = my_slicer.data \
         .dimension(
            # Select the date dimension with a daily interval to group the data by the day applies to
            # dimensions are referenced by `slicer.dimensions.{alias}`
            my_slicer.dimensions.date(daily),

            # Select the device_type dimension to break the data down further by which device it applies to
            my_slicer.dimensions.device_type,
         ) \
         .filter(
            # Filter the result set to data to the year of 2018
            my_slicer.dimensions.date.between(date(2018, 1, 1), date(2018, 12, 31))
         ) \
         # Add a week over week reference to compare data to values from the week prior
         .reference(WeekOverWeek(slicer.dimension.date))
         .widget(
            # Add a matpotlib chart widget
            Matplotlib()
               # Add axes with series to the chart
               .axis(Matplotlib.LineSeries(slicer.metrics.clicks))

               # metrics are referenced by `slicer.metrics.{alias}`
               .axis(Matplotlib.ColumnSeries(slicer.metrics.cost, slicer.metrics.revenue))
         ) \
         .widget(
            # Add a pandas data frame table widget
            Pandas(slicer.metrics.clicks, slicer.metrics.cost, slicer.metrics.revenue)
         ) \
         .fetch()

    # Display the chart
    matplotlib_chart.plot()

    # Display the chart
    print(pandas_df)

.. _slicer_query_example_end:

License
-------

Copyright 2018 KAYAK Germany, GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Crafted with ♥ in Berlin.

.. _license_end:


.. _available_badges_start:

.. |BuildStatus| image:: https://travis-ci.org/kayak/fireant.svg?branch=master
   :target: https://travis-ci.org/kayak/fireant
.. |CoverageStatus| image:: https://coveralls.io/repos/kayak/fireant/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/kayak/fireant?branch=master
.. |Codacy| image:: https://api.codacy.com/project/badge/Grade/832b5a7dda8949c3b2ede28deada4569
   :target: https://www.codacy.com/app/twheys/fireant
.. |Docs| image:: https://readthedocs.org/projects/fireant/badge/?version=latest
   :target: http://fireant.readthedocs.io/en/latest/
.. |PyPi| image:: https://img.shields.io/pypi/v/fireant.svg?style=flat
   :target: https://pypi.python.org/pypi/fireant
.. |License| image:: https://img.shields.io/hexpm/l/plug.svg?maxAge=2592000
   :target: http://www.apache.org/licenses/LICENSE-2.0

.. _available_badges_end:

.. _appendix_start:

.. |Brand| replace:: *fireant*

.. |FeatureSlicer| replace:: *Slicer*
.. |FeatureMetric| replace:: *Metric*
.. |FeatureDimension| replace:: *Dimension*
.. |FeatureFilter| replace:: *Filter*
.. |FeatureReference| replace:: *Reference*
.. |FeatureOperation| replace:: *Operation*

.. |ClassSlicer| replace:: ``fireant.Slicer``
.. |ClassDatabase| replace:: ``fireant.database.Database``
.. |ClassJoin| replace:: ``fireant.slicer.joins.Join``
.. |ClassMetric| replace:: ``fireant.slicer.metrics.Metric``

.. |ClassDimension| replace:: ``fireant.slicer.dimensions.Dimension``
.. |ClassBooleanDimension| replace:: ``fireant.slicer.dimensions.BooleanDimension``
.. |ClassContDimension| replace:: ``fireant.slicer.dimensions.ContinuousDimension``
.. |ClassDateDimension| replace:: ``fireant.slicer.dimensions.DatetimeDimension``
.. |ClassCatDimension| replace:: ``fireant.slicer.dimensions.CategoricalDimension``
.. |ClassUniqueDimension| replace:: ``fireant.slicer.dimensions.UniqueDimension``
.. |ClassDisplayDimension| replace:: ``fireant.slicer.dimensions.DisplayDimension``

.. |ClassFilter| replace:: ``fireant.slicer.filters.Filter``
.. |ClassComparatorFilter| replace:: ``fireant.slicer.filters.ComparatorFilter``
.. |ClassBooleanFilter| replace:: ``fireant.slicer.filters.BooleanFilter``
.. |ClassContainsFilter| replace:: ``fireant.slicer.filters.ContainsFilter``
.. |ClassExcludesFilter| replace:: ``fireant.slicer.filters.ExcludesFilter``
.. |ClassRangeFilter| replace:: ``fireant.slicer.filters.RangeFilter``
.. |ClassPatternFilter| replace:: ``fireant.slicer.filters.PatternFilter``
.. |ClassAntiPatternFilter| replace:: ``fireant.slicer.filters.AntiPatternFilter``

.. |ClassWidget| replace:: ``fireant.slicer.widgets.base.Widget``
.. |ClassPandasWidget| replace:: ``fireant.slicer.widgets.pandas.Pandas``
.. |ClassHighChartsWidget| replace:: ``fireant.slicer.widgets.highcharts.HighCharts``
.. |ClassHighChartsSeries| replace:: ``fireant.slicer.widgets.highcharts.Series``

.. |ClassReference| replace:: ``fireant.slicer.references.Reference``

.. |ClassOperation| replace:: ``fireant.slicer.operations.Operation``

.. |ClassVerticaDatabase| replace:: ``fireant.database.VerticaDatabase``
.. |ClassMySQLDatabase| replace:: ``fireant.database.MySQLDatabase``
.. |ClassPostgreSQLDatabase| replace:: ``fireant.database.PostgreSQLDatabase``
.. |ClassRedshiftDatabase| replace:: ``fireant.database.RedshiftDatabase``

.. |ClassDatetimeInterval| replace:: ``fireant.DatetimeInterval``

.. |ClassTable| replace:: ``pypika.Table``
.. |ClassTables| replace:: ``pypika.Tables``

.. _PyPika: https://github.com/kayak/pypika/
.. _Pandas: http://pandas.pydata.org/
.. _Jupyter: http://jupyter.org/
.. _Matplotlib: http://matplotlib.org/
.. _HighCharts: http://www.highcharts.com/
.. _Datatables: https://datatables.net/
.. _React-Table: https://react-table.js.org/

.. _appendix_end:
