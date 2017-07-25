Dashboards and Reports
======================

.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:

|Brand| offers higher level features that leverage the |FeatureSlicer| for more complex requests in order to create dashboards and reports.  A |FeatureWidgetGroup| is a layer of abstraction on top of the |FeatureSlicer| which facilitates the configuration of groups of widgets such as charts and tables.  The |FeatureWidgetGroup| then provides an API for querying data for the whole group.

Widgets
-------

Each |FeatureWidgetGroup| contains one or more |FeatureWidget| which represents one chart, table or other type of UI component.  Each |FeatureWidget| will display a subset of the data from the combined result of a |FeatureSlicer| request made by the |FeatureWidgetGroup|.  Widgets define which metrics they display and dimensions, filters, references, and operations are applied to the entire group.

Additionally, widgets can display a subset of values for a dimension.  This is useful in reports where a several widgets display the same metrics but one widget per dimension value is used.  For example, a chart could display the metrics *clicks* and *conversions* with a |ClassCatDimension| for *device* so that there is one chart for each type of device, desktop, tablet, and mobile.


.. _config_dashboard_start:


Configuring a Widget Group
--------------------------

Here is a concrete example of a |FeatureWidgetGroup| configuration. The |FeatureWidgetGroup| at a minimum defines a |ClassSlicer| and a list containing *at least one* |ClassWidget|.

In addition, dimensions, filters, references, and operations can be defined.  Any of these are applied to all widgets in the widget group and are implicitly and always present in any request of the |ClassWidgetGroupManager|.

Sometimes it is necessary to include these, for example a Line Chart widget will always require a continuous dimension as its first dimension, in the |ClassWidgetGroup| one can be defined in order to avoid having to constantly pass it as a parameter to the |ClassWidgetGroupManager|.  Dimensions, filters, references, and operations behave exactly the same in the |ClassWidgetGroup| schema as they do in the |ClassSlicerManager| as well as in the |ClassWidgetGroupManager| API.


.. code-block:: python

    from fireant.slicer import *
    from fireant.dashboards import *

    my_widget_group = WidgetGroup(
        my_slicer,

        widgets=[
            LineChartWidget(metrics=['clicks', 'searches']),
            LineChartWidget(metrics=['rpc', 'cpc']),
            LineChartWidget(metrics=['roi']),
        ],

        dimensions=['date'],

        filters=[
            RangeFilter('date', date.today() - timedelta(90), date.today())
        ],

        references=[WoW('date')]
    )


.. _config_dashboard_end:

Using the Widget Group Manager
------------------------------

The |ClassWidgetGroupManager| provides an API similar to that of the |ClassSlicerManager| but combines the requirements of all of its widgets into one slicer request and then transforms the resulting data appropriately for each widget.  The ``render`` function accepts the same parameters as the functions of the |ClassSlicerManager| except for ``metrics``, but additionally will prepend any parameters provided to the |ClassWidgetGroup| constructor.

The ``render`` function of the |ClassWidgetGroupManager| will return an array containing python ``dict`` containing the data for each widget in the respective format.  The ``render`` function can be called without any parameters, which will use only what is defined in the |ClassWidgetGroup| in the request.

 The following example will return an array containing data for the widgets defined about, each in the Highcharts_ line chart format.  Each of them will have the *date* dimension, the *date range filter*, and the *Week over Week* reference applied to them.

.. code-block:: python

    result = my_widget_group.manager.render()


Additional dimensions, filters, references, and operations can be added to the request in the render function and behave similarly to the |ClassSlicerManager| API, except that anything defined in the |ClassWidgetGroupManager| constructor will be prepended to the parameters provided in the API.

.. code-block:: python

    result = my_widget_group.manager.render(
        dimensions=[
            'locale'
        ],
    )

.. code-block:: python

    result = my_widget_group.manager.render(
        dimension_filters=[
            EqualityFilter('device', EqualityOperator.eq, 'mobile')
        ],
    )

.. code-block:: python

    result = my_widget_group.manager.render(
        references=[
            Delta.WoW('date')
        ],
    )

Note: Pagination can also be used on widget groups. To enable pagination, just pass a ``fireant.slicer.pagination.Paginator``
object into the render function using the pagination kwarg. For further information on this, please see the Slicer Pagination documentation.

.. code-block:: python

    from pypika import Order

    from fireant.slicer import Paginator
    result = my_widget_group.manager.render(
        paginator=Paginator(limit=50, order=[('locale', Order.desc)])
    )
