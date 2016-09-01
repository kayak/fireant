# coding: utf-8
from fireant import utils


class WidgetGroupManager(object):
    def __init__(self, widget_group):
        self.widget_group = widget_group

    def render(self, dimensions=None, metric_filters=None, dimension_filters=None, references=None, operations=None):
        combined_dimensions = utils.filter_duplicates(self.widget_group.dimensions + (dimensions or []))

        dataframe = self.widget_group.slicer.manager.data(
            metrics=[metric
                     for widget in self.widget_group.widgets
                     for metric in widget.metrics],
            dimensions=combined_dimensions,
            metric_filters=metric_filters or [],
            dimension_filters=self.widget_group.dimension_filters + (dimension_filters or []),
            references=self.widget_group.references + (references or []),
            operations=self.widget_group.operations + (operations or []),
        )

        return self._transform_widgets(self.widget_group.widgets, dataframe, combined_dimensions)

    def _transform_widgets(self, widgets, dataframe, dimensions):
        for widget in widgets:
            display_schema = self.widget_group.slicer.manager.display_schema(
                metrics=widget.metrics,
                dimensions=dimensions
            )
            widget_df = utils.correct_dimension_level_order(dataframe[widget.metrics], display_schema)
            yield widget.transformer.transform(
                widget_df,
                display_schema
            )
