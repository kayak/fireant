# coding: utf-8

import functools

from pypika import functions as fn
from .queries import QueryManager
from .transformers import (TableIndex, HighchartsTransformer, HighchartsColumnTransformer, DataTablesTransformer)

_default_transformers = {
    'line_chart': HighchartsTransformer(),
    'column_chart': HighchartsColumnTransformer(HighchartsColumnTransformer.column),
    'bar_chart': HighchartsColumnTransformer(HighchartsColumnTransformer.bar),
    'row_index_table': DataTablesTransformer(TableIndex.row_index),
    'column_index_table': DataTablesTransformer(TableIndex.column_index),
}


class SlicerException(Exception):
    pass


class SlicerManager(QueryManager):
    def __init__(self, slicer, transformers=None):
        """
        :param slicer:
        :param transformers:
        """
        self.slicer = slicer

        # Creates a function on the slicer for each transformer
        for tx_key, tx in (transformers or _default_transformers).items():
            setattr(self, tx_key, functools.partial(self._get_and_transform_data, tx))

    def _get_and_transform_data(self, tx, metrics=tuple(), dimensions=tuple(),
                                metric_filters=tuple(), dimension_filters=tuple(),
                                references=tuple(), operations=tuple()):
        # Loads data and transforms it with a given transformer.
        df = self.data(metrics=metrics, dimensions=dimensions,
                       metric_filters=metric_filters, dimension_filters=dimension_filters,
                       references=references, operations=operations)
        display_schema = self.get_display_schema(metrics, dimensions, references)
        return tx.transform(df, display_schema)

    def data(self, metrics=tuple(), dimensions=tuple(),
             metric_filters=tuple(), dimension_filters=tuple(),
             references=tuple(), operations=tuple()):
        """
        :param metrics:
            Type: list or tuple
            A set of metrics to include in the query.

        :param dimensions:
            Type: list or tuple
            A set of dimensions to split the metrics into groups.

        :param metric_filters:
            Type: list or tuple
            A set of filters to constrain the data with by metric thresholds.

        :param dimension_filters:
            Type: list or tuple
            A set of filters to constrain the data with by dimension.

        :param references:
            Type: list or tuple
            A set of comparisons to include in the query.

        :param operations:
            Type: list or tuple
            A set of operations to perform on the response.

        :return:
            A transformed response that is queried based on the slicer and the format.
        """
        from fireant import settings
        if settings.database is None:
            raise SlicerException('Unable to execute queries until a database is configured.  Please import '
                                  '`fireant.settings` and set some value to `settings.database`.')

        query_schema = self.get_query_schema(metrics=metrics, dimensions=dimensions,
                                             metric_filters=metric_filters, dimension_filters=dimension_filters,
                                             references=references, operations=operations)

        data_frame = self._query_data(**query_schema)

        # TODO add post processing
        # operations_schema = self._get_operations_schema(operations)
        # data_frame = self.post_process(
        #     data_frame,
        #     operations_schema,
        # )

        # do post-processing ops

        return data_frame

    def get_query_schema(self, metrics=None, dimensions=None,
                         metric_filters=None, dimension_filters=None,
                         references=None, operations=None):

        if not metrics:
            raise ValueError('Invalid slicer request.  At least one metric is required to build a query.')

        schema_metrics, metrics_joins = self._metrics_schema(metrics)
        schema_dimensions, dimensions_joins = self._dimensions_schema(dimensions or [])
        schema_joins = self._joins_schema(metrics_joins | dimensions_joins)

        return {
            'table': self.slicer.table,
            'joins': schema_joins,
            'metrics': schema_metrics,
            'dimensions': schema_dimensions,
            'mfilters': self._filters_schema(self.slicer.metrics, metric_filters or [], self._default_metric_definition,
                                             element_label='metric'),
            'dfilters': self._filters_schema(self.slicer.dimensions, dimension_filters or [],
                                             self._default_dimension_definition),
            'references': self._references_schema(references, dimensions or [], schema_dimensions),
            'rollup': [dimension
                       for operation in operations or []
                       if 'rollup' == operation.key
                       for dimension in operation.schemas(self.slicer)],
        }

    def get_display_schema(self, metrics=None, dimensions=None, references=None):
        """
        Builds a display schema for

        :param metrics:
            Type: list[str]
            The requested list of metrics.  This must match a Metric contained in the slicer.
        :param dimensions:
            Type: list[str or tuple/list]
            The requested list of dimensions.  For simple dimensions, the string key is passed as a parameter, otherwise
            a tuple of list with the first element equal to the key and the subsequent elements equal to the paramters.
        :param references:
            Type: list[tuple]
            A list of tuples describing reference options, where the first element in the tuple is the key of the
            reference operation and the second value is the dimension to perform the comparasion along.
        :return:
            A dictionary describing how to transform the resulting data frame for the same request.
        """
        return {
            'metrics': {key: self.slicer.metrics[key].label
                        for key in metrics or []},
            'dimensions': self._display_dimensions(dimensions),
            'references': [reference.key for reference in references or []]
        }

    def _metrics_schema(self, keys):
        joins = set()
        metrics = {}
        for metric in keys:
            schema_metric = self.slicer.metrics.get(metric)

            if not schema_metric:
                raise ValueError('Invalid metric key [%s]' % metric)

            for key, definition in schema_metric.schemas():
                metrics[key] = definition or self._default_metric_definition(key)

            if schema_metric.joins:
                joins |= set(schema_metric.joins)

        return metrics, joins

    def _dimensions_schema(self, keys):
        joins = set()
        dimensions = {}
        for dimension in keys:
            # unpack tuples for args
            if isinstance(dimension, (list, tuple)):
                dimension, args = dimension[0], dimension[1:]
            else:
                args = []

            schema_dimension = self.slicer.dimensions.get(dimension)

            if not schema_dimension:
                raise ValueError('Invalid dimension key [%s]' % dimension)

            for key, definition in schema_dimension.schemas(*args):
                dimensions[key] = definition or self._default_dimension_definition(key)

            if schema_dimension.joins:
                joins |= set(schema_dimension.joins)

        return dimensions, joins

    def _joins_schema(self, keys):
        return [(self.slicer.joins[key].table, self.slicer.joins[key].criterion, self.slicer.joins[key].join_type)
                for key in keys]

    def _filters_schema(self, elements, filters, default_value_func, element_label='dimension'):
        filters_schema = []
        for f in filters:
            if '.' in f.element_key:
                element_key, modifier = f.element_key.split('.')
            else:
                element_key, modifier = f.element_key, None

            element = elements.get(element_key)
            if not element:
                raise SlicerException(
                    'Unable to apply filter [{filter}].  '
                    'No such {element} with key [{key}].'.format(
                        filter=f,
                        element=element_label,
                        key=f.element_key
                    ))

            if 'label' == modifier:
                definition = element.label_field

            else:
                definition = element.definition or default_value_func(element.key)

            filters_schema.append(f.schemas(definition))

        return filters_schema

    def _display_dimensions(self, dimensions):
        req_dimension_keys = [dimension[0]
                              if isinstance(dimension, (tuple, list))
                              else dimension
                              for dimension in dimensions or []]

        display_dims = []
        for key in req_dimension_keys:
            dimension = self.slicer.dimensions[key]
            display_dim = {'label': dimension.label}

            if hasattr(dimension, 'options'):
                display_dim['label_options'] = {opt.key: opt.label
                                                for opt in dimension.options}

            if hasattr(dimension, 'id_fields'):
                id_fields = ['{}_id{}'.format(dimension.key, i)
                             for i in range(len(dimension.id_fields))]
                display_dim['id_fields'] = id_fields
                display_dim['label_field'] = '%s_label' % dimension.key
            else:
                display_dim['id_fields'] = [dimension.key]

            display_dims.append(display_dim)

        return display_dims

    def _references_schema(self, references, dimensions, schema_dimensions):
        dimension_keys = {dimension[0] if isinstance(dimension, (list, tuple, set)) else dimension
                          for dimension in dimensions}

        schema_references = []
        for reference in references or []:
            if reference.element_key not in dimension_keys:
                raise SlicerException(
                    'Unable to query with [{reference}]. '
                    'Dimension [{dimension}] is missing.'.format(reference=str(reference),
                                                                 dimension=reference.element_key))

            from .schemas import DatetimeDimension
            if not isinstance(self.slicer.dimensions[reference.element_key], DatetimeDimension):
                raise SlicerException(
                    'Unable to query with [{reference}]. '
                    'Dimension [{dimension}] must be a DatetimeDimension.'.format(reference=str(reference),
                                                                                  dimension=reference.element_key))

            schema_references.append((reference.key, reference.element_key))

        return schema_references

    def _default_dimension_definition(self, key):
        return fn.Coalesce(self.slicer.table.field(key), 'null')

    def _default_metric_definition(self, key):
        return fn.Sum(self.slicer.table.field(key))
