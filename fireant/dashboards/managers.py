# coding: utf-8
from fireant.slicer.transformers import utils as tx_utils


class WidgetGroupManager(object):
    def __init__(self, widget_group):
        self.widget_group = widget_group

    def render(self, dimensions=None, metric_filters=None, dimension_filters=None, references=None, operations=None):
        enabled_metrics = self._filter_duplicates(metric
                                                  for widget in self.widget_group.widgets
                                                  for metric in widget.metrics)
        enabled_dimensions = self._filter_duplicates(self.widget_group.dimensions + (dimensions or []))
        enabled_dfilters = self._filter_duplicates(self.widget_group.dimension_filters + (dimension_filters or []))
        enabled_references = self._filter_duplicates(self.widget_group.references + (references or []))
        enabled_operations = self._filter_duplicates(self.widget_group.operations + (operations or []))

        dataframe = self.widget_group.slicer.manager.data(
            metrics=enabled_metrics,
            dimensions=enabled_dimensions,
            metric_filters=metric_filters or [],
            dimension_filters=enabled_dfilters,
            references=enabled_references,
            operations=enabled_operations,
        )

        return self._transform_widgets(self.widget_group.widgets, dataframe, enabled_dimensions)

    @staticmethod
    def _filter_duplicates(iterable):
        filtered_list, seen = [], set()
        for x in iterable:
            key = x[0] if isinstance(x, (list, tuple, set)) else x

            if key in seen:
                continue

            seen.add(key)
            filtered_list.append(x)

        return filtered_list

    def _transform_widgets(self, widgets, dataframe, dimensions):
        for widget in widgets:
            display_schema = self.widget_group.slicer.manager.display_schema(
                metrics=widget.metrics,
                dimensions=dimensions
            )
            widget_df = tx_utils.correct_dimension_level_order(dataframe[widget.metrics], display_schema)
            yield widget.transformer.transform(
                widget_df,
                display_schema
            )
