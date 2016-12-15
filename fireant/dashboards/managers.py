# coding: utf-8
import pandas as pd

from fireant import utils


class WidgetGroupManager(object):
    def __init__(self, widget_group):
        self.widget_group = widget_group

    def render(self, dimensions=None, metric_filters=None, dimension_filters=None, references=None, operations=None):
        combined_dimensions = utils.filter_duplicates(self.widget_group.dimensions + (dimensions or []))
        combined_references = utils.filter_duplicates(self.widget_group.references + (references or []))
        combined_operations = utils.filter_duplicates(self.widget_group.operations + (operations or []))

        dataframe = self.widget_group.slicer.manager.data(
            metrics=[metric
                     for widget in self.widget_group.widgets
                     for metric in utils.flatten(widget.metrics)],
            dimensions=combined_dimensions,
            metric_filters=metric_filters or [],
            dimension_filters=self.widget_group.dimension_filters + (dimension_filters or []),
            references=combined_references,
            operations=combined_operations,
        )

        return list(self._transform_widgets(self.widget_group.widgets, dataframe,
                                            combined_dimensions, combined_references))

    def _transform_widgets(self, widgets, dataframe, dimensions, references):
        for widget in widgets:
            display_schema = self.widget_group.slicer.manager.display_schema(
                metrics=widget.metrics,
                dimensions=dimensions,
                references=references,
            )

            if references:
                # This escapes a pandas bug where a data frame subset of columns still returns the columns of the
                # original data frame
                reference_keys = [''] + [ref.key for ref in references]
                subset_columns = pd.MultiIndex.from_product([reference_keys, utils.flatten(widget.metrics)])
                subset = pd.DataFrame(dataframe[subset_columns], columns=subset_columns)

            else:
                subset = dataframe[utils.flatten(widget.metrics)]

            yield widget.transformer.transform(subset, display_schema)
