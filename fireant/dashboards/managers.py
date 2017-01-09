# coding: utf-8
import pandas as pd

from fireant import utils
from fireant.slicer.managers import Totals


class WidgetGroupManager(object):
    def __init__(self, widget_group):
        self.widget_group = widget_group

    def _schema(self, dimensions=None, metric_filters=None, dimension_filters=None, references=None, operations=None):
        combined_dimensions = utils.filter_duplicates(self.widget_group.dimensions + (dimensions or []))
        combined_references = utils.filter_duplicates(self.widget_group.references + (references or []))
        combined_operations = utils.filter_duplicates(self.widget_group.operations + (operations or []))

        return dict(
            metrics=[metric
                     for widget in self.widget_group.widgets
                     for metric in utils.flatten(widget.metrics)],
            dimensions=combined_dimensions,
            metric_filters=metric_filters or [],
            dimension_filters=self.widget_group.dimension_filters + (dimension_filters or []),
            references=combined_references,
            operations=combined_operations,
        )

    def render(self, dimensions=None, metric_filters=None, dimension_filters=None, references=None, operations=None):
        schema = self._schema(dimensions, metric_filters, dimension_filters, references, operations)
        dataframe = self.widget_group.slicer.manager.data(**schema)
        return list(
            self._transform_widgets(self.widget_group.widgets, dataframe,
                                    schema['dimensions'], schema['references'], schema['operations'])
        )

    def query_string(self, dimensions=None, metric_filters=None, dimension_filters=None, references=None,
                     operations=None):
        schema = self._schema(dimensions, metric_filters, dimension_filters, references, operations)
        return self.widget_group.slicer.manager.query_string(**schema)


    def _transform_widgets(self, widgets, dataframe, dimensions, references, operations):
        for widget in widgets:
            display_schema = self.widget_group.slicer.manager.display_schema(
                metrics=widget.metrics,
                dimensions=dimensions,
                references=references,
                operations=operations,
            )

            # Temporary fix to enable operations to get output properly. Can removed when the Fireant API is refactored.
            operation_columns = ['{}_{}'.format(operation.metric_key, operation.key)
                                 for operation in operations if operation.key != Totals.key]

            columns = utils.flatten(widget.metrics) + operation_columns

            if references:
                # This escapes a pandas bug where a data frame subset of columns still returns the columns of the
                # original data frame
                reference_keys = [''] + [ref.key for ref in references]
                subset_columns = pd.MultiIndex.from_product([reference_keys, columns])
                subset = pd.DataFrame(dataframe[subset_columns], columns=subset_columns)

            else:
                subset = dataframe[columns]

            yield widget.transformer.transform(subset, display_schema)
