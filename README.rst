FireAnt - Analytics and Reporting
=================================

.. _intro_start:

|BuildStatus|  |CoverageStatus|  |Codacy|  |Docs|  |PyPi|  |License|


|Brand| is a a data analysis tool used for quickly building charts, tables, reports, and dashboards.  It defines a schema for configuring metrics and dimensions which removes most of the leg work of writing queries and formatting charts.  |Brand| even works great with Jupyter notebooks and in the Python shell providing quick and easy access to your data.

.. _intro_end:

Read more at http://fireant.readthedocs.io/en/latest/

Installation
------------

.. _installation_start:

To install |Brand|, run the following command in the terminal:

.. code-block:: bash

    pip install fireant


.. _installation_end:

Slicers
-------

Slicers are the core component of |Brand|.  A Slicer is a configuration of two types of elements, metrics and dimensions, which represent what kinds of data exist and how the data can be organized.  A metric is a type of data, a measurement such as clicks and a dimension is a range over which metrics can be extended or grouped by.  Concretely, metrics represent the data *in* a chart or table and dimensions represent the rows and columns, axes, or categories.

To configure a slicer, instantiate a |ClassSlicer| with a list of |ClassMetric| and |ClassDimension|.

.. _slicer_example_start:

.. code-block:: python

    from fireant.slicer import *
    from pypika import Tables, functions as fn

    analytics, accounts = Tables('analytics', 'accounts')

    my_slicer = Slicer(
        # This is the primary database table that our slicer uses
        table=analytics,

        joins=[
            # Metrics and dimensions can use columns from joined tables by
            # configuring the join here.  Joins will only be used when necessary.
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
            # Datetime Dimensions are continuous and must be rounded to an interval
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


Querying Data and Rendering Charts
----------------------------------

Once a slicer is configured, it is ready to be used.  Each slicer comes with a |ClassSlicerManager| and several |ClassTransformerManager| which expose an interface for executing queries and transforming the results.  Each function in the manager uses the same signature.  The principal function is ``data`` and all othe functions call this function first.  The additional functions provide a transformation to the data.

The notebooks transformer bundle includes different functions for use in Jupyter_ notebooks.  Other formats return results in JSON format.

.. _manager_api_start:

* ``my_slicer.manager.data`` - A Pandas_ data frame indexed by the selected dimensions.

* ``my_slicer.notebooks.row_index_table`` - A Datatables_ row-indexed table.
* ``my_slicer.notebooks.column_index_table`` - A Datatables_ column-indexed table.

* ``my_slicer.notebooks.line_chart`` - A Matplotlib_ line chart. (Requires [matplotlib] dependency)
* ``my_slicer.notebooks.column_chart`` - A Matplotlib_ column chart. (Requires [matplotlib] dependency)
* ``my_slicer.notebooks.bar_chart`` - A Matplotlib_ bar chart. (Requires [matplotlib] dependency)

* ``my_slicer.highcharts.line_chart`` - A Highcharts_ line chart.
* ``my_slicer.highcharts.column_chart`` - A Highcharts_ column chart.
* ``my_slicer.highcharts.bar_chart`` - A Highcharts_ bar chart.

* ``my_slicer.datatables.row_index_table`` - A Datatables_ row-indexed table.
* ``my_slicer.datatables.column_index_table`` - A Datatables_ column-indexed table.

.. code-block:: python

    def data(self, metrics, dimensions, metric_filters, dimension_filters, references, operations):

.. _manager_api_end:

Examples
--------

Use the ``data`` function to get a Pandas_ data frame or series.  The following example will result in a data frame with 'device' as the index, containing the values 'Desktop', 'Tablet', and 'Mobile', and the columns 'Clicks' and 'ROI'.

.. code-block:: python

    df = my_slicer.manager.data(
        metrics=['clicks', 'roi'],
        dimensions=['device']
    )

Removing the dimension will yield a similar result except as a Pandas_ series containing 'Clicks' and 'ROI'.  These are the aggregated values over the entire data base table.

.. code-block:: python

    df = my_slicer.manager.data(
        metrics=['clicks', 'roi'],
    )

The transformer functions us the data function but then apply a transformation to convert the data into formats for Highcharts_ or Datatables_.  The results for these can be serialized directly into json objects.


.. code-block:: python

    import json

    result = my_slicer.manager.line_chart(
        metrics=['clicks', 'roi'],
        dimensions=['date', 'device'],
    )

    json.dumps(result)


.. code-block:: python

    import json

    result = my_slicer.manager.row_index_table(
        metrics=['clicks', 'revenue', 'cost', 'roi'],
        dimensions=['account', 'device'],
    )

    json.dumps(result)


License
-------

Copyright 2016 KAYAK Germany, GmbH

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
.. |FeatureTransformer| replace:: *Transformer*

.. |FeatureWidgetGroup| replace:: *Dashboard*
.. |FeatureWidget| replace:: *Section*

.. |ClassSlicer| replace:: ``fireant.Slicer``
.. |ClassSlicerManager| replace:: ``fireant.slicer.SlicerManager``
.. |ClassMetric| replace:: ``fireant.slicer.Metric``
.. |ClassDimension| replace:: ``fireant.slicer.Dimension``

.. |ClassContDimension| replace:: ``fireant.slicer.ContinuousDimension``
.. |ClassDateDimension| replace:: ``fireant.slicer.DatetimeDimension``
.. |ClassCatDimension| replace:: ``fireant.slicer.CategoricalDimension``
.. |ClassUniqueDimension| replace:: ``fireant.slicer.UniqueDimension``

.. |ClassWidgetGroup| replace:: ``fireant.dashboards.WidgetGroup``
.. |ClassWidget| replace:: ``fireant.dashboards.Widget``
.. |ClassWidgetGroupManager| replace:: ``fireant.dashboards.WidgetGroupManager``

.. |ClassEqualityFilter| replace:: ``fireant.slicer.EqualityFilter``
.. |ClassContainsFilter| replace:: ``fireant.slicer.ContainsFilter``
.. |ClassRangeFilter| replace:: ``fireant.slicer.RangeFilter``
.. |ClassFuzzyFilter| replace:: ``fireant.slicer.FuzzyFilter``

.. |ClassFilter| replace:: ``fireant.slicer.Filter``
.. |ClassReference| replace:: ``fireant.slicer.Reference``
.. |ClassOperation| replace:: ``fireant.slicer.Operation``

.. |ClassDashboard| replace:: ``fireant.Dashboard``
.. |ClassSection| replace:: ``fireant.dashboards.Section``

.. |ClassDatabase| replace:: ``fireant.database.Database``
.. |ClassVertica| replace:: ``fireant.database.Vertica``

.. _PyPika: https://github.com/kayak/pypika/
.. _Pandas: http://pandas.pydata.org/
.. _Jupyter: http://jupyter.org/
.. _Matplotlib: http://matplotlib.org/
.. _Highcharts: http://www.highcharts.com/
.. _Datatables: https://datatables.net/

.. _appendix_end:
