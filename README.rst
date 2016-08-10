Installation
------------

.. _intro_start:

|BuildStatus|  |CoverageStatus|  |Codacy|  |Docs|  |PyPi|  |License|

Abstract
========

|Brand| is a a data analysis tool used for quickly building charts, tables, reports, and dashboards.  It provides a
schema for configuring data access, for example which metrics can be queried and which dimensions can be grouped on.
An API is provided for making requests with this schema which enables filtering, comparisons, and post-processing
operations.  The Dashboards feature provides an extra level of abstraction and empowers the assembly of reports
containing multiple charts, tables, and widgets and facilitates quickly viewing your data from every angle.

.. _intro_end:

.. _installation_start:

To install |Brand|, run the following command in the terminal:

.. code-block:: bash

    pip install fireant


.. _installation_end:

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

.. |Brand| replace:: *FireAnt*

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
.. _Highcharts: http://www.highcharts.com/
.. _Datatables: https://datatables.net/

.. _appendix_end:
