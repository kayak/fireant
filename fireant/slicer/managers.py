# coding: utf-8

import functools

from collections import OrderedDict
from fireant.slicer.operations import Totals
from fireant import utils
from pypika import functions as fn

from .postprocessors import OperationManager
from .queries import QueryManager


class SlicerException(Exception):
    pass


class SlicerManager(QueryManager, OperationManager):
    def __init__(self, slicer):
        """
        :param slicer:
        """
        self.slicer = slicer

    def data(self, metrics=(), dimensions=(),
             metric_filters=(), dimension_filters=(),
             references=(), operations=()):
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
        metrics = utils.filter_duplicates(metrics)
        dimensions = utils.filter_duplicates(dimensions)

        query_schema = self.data_query_schema(metrics=metrics, dimensions=dimensions,
                                              metric_filters=metric_filters, dimension_filters=dimension_filters,
                                              references=references, operations=operations)
        operation_schema = self.operation_schema(operations)

        dataframe = self.query_data(**query_schema)
        return self.post_process(dataframe, operation_schema)

    def dimension_options(self, dimension, filters, limit=None):
        dimopt_schema = self.dimension_option_schema(dimension, filters, limit)
        return self.query_dimension_options(**dimopt_schema)

    def data_query_schema(self, metrics=(), dimensions=(),
                          metric_filters=(), dimension_filters=(),
                          references=(), operations=()):
        """
        Builds a `dict` model of the schema parts required for executing a data query given a request.

        :param metrics:
        :param dimensions:
        :param metric_filters:
        :param dimension_filters:
        :param references:
        :param operations:

        :return:
        """

        metric_joins_schema = self._joins_schema(set(metrics) | {mf.element_key for mf in metric_filters},
                                                 self.slicer.metrics)
        dimension_joins_schema = self._joins_schema(set(dimensions) | {df.element_key for df in dimension_filters},
                                                    self.slicer.dimensions)
        return {
            'database': self.slicer.database,
            'table': self.slicer.table,

            'metrics': self._metrics_schema(metrics, operations),
            'dimensions': self._dimensions_schema(dimensions),

            'mfilters': self._filters_schema(self.slicer.metrics, metric_filters,
                                             self._default_metric_definition,
                                             element_label='metric'),
            'dfilters': self._filters_schema(self.slicer.dimensions,
                                             dimension_filters,
                                             self._default_dimension_definition),

            'joins': list(metric_joins_schema | dimension_joins_schema),
            'references': self._references_schema(references, dimensions),
            'rollup': self._totals_schema(dimensions, operations),
        }

    def dimension_option_schema(self, dimension, filters, limit=None):
        dimensions = [dimension]

        schema_dimensions = self._dimensions_schema(dimensions)
        schema_filters = self._filters_schema(self.slicer.dimensions, filters, self._default_dimension_definition)
        schema_joins = self._joins_schema(set(dimensions) | {df.element_key for df in filters},
                                          self.slicer.dimensions)

        return {
            'database': self.slicer.database,
            'table': self.slicer.hint_table or self.slicer.table,
            'joins': schema_joins,
            'dimensions': schema_dimensions,
            'filters': schema_filters,
            'limit': limit,
        }

    def display_schema(self, metrics=(), dimensions=(), references=(), operations=()):
        """
        Builds a display schema for a request.  This is used in combination with the results of the query that is
        executed by the Slicer manager to transform the results with display labels.  The display schema carries
        meta-data pertaining to displaying the results of the query.

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
            'metrics': self._display_metrics(metrics, operations),
            'dimensions': self._display_dimensions(dimensions, operations),
            'references': OrderedDict([(reference.key, reference.label)
                                       for reference in references]),
        }

    def operation_schema(self, operations):
        results = []
        for operation in operations:
            schema = operation.schemas()

            if schema is not None:
                results.append(schema)

        return results

    def _metrics_schema(self, metrics=(), operations=()):
        keys = list(metrics) + [metric
                                for op in operations
                                for metric in op.metrics()]

        if not keys:
            raise SlicerException('At least one metric is required requests.  Please add a metric.')

        invalid_metrics = {utils.slice_first(key)
                           for key in keys} - set(self.slicer.metrics)
        if invalid_metrics:
            raise SlicerException('Invalid metrics included in request: '
                                  '[%s]' % ', '.join(invalid_metrics))

        schema_metrics = OrderedDict()
        for key in keys:
            schema_metric = self.slicer.metrics.get(key)

            for metric_key, definition in schema_metric.schemas():
                schema_metrics[metric_key] = definition or self._default_metric_definition(metric_key)

        return schema_metrics

    def _dimensions_schema(self, keys):
        invalid_dimensions = {utils.slice_first(key) for key in keys} - set(self.slicer.dimensions)
        if invalid_dimensions:
            raise SlicerException('Invalid dimensions included in request: '
                                  '[%s]' % ', '.join(invalid_dimensions))

        dimensions = OrderedDict()
        for dimension in keys:
            # unpack tuples for args
            if isinstance(dimension, (list, tuple)):
                dimension, args = dimension[0], dimension[1:]
            else:
                args = []

            schema_dimension = self.slicer.dimensions.get(dimension)

            for key, definition in schema_dimension.schemas(*args, database=self.slicer.database):
                dimensions[key] = definition or self._default_dimension_definition(key)

        return dimensions

    def _joins_schema(self, keys, elements):
        """

        :param keys:
            The keys of the schema elements to retrieve joins for.
        :param elements:
            The elements to retrieve the joins from, either slicer.metrics or slicer.dimensions.
        :return:
            A `set` of join schemas containing the join table and the join criterion.
        """
        joins = set()

        for key in keys:
            element = elements.get(utils.slice_first(key))
            if element and element.joins:
                joins |= set(element.joins)

        return {(self.slicer.joins[key].table,
                 self.slicer.joins[key].criterion,
                 self.slicer.joins[key].join_type) for key in joins}

    def _filters_schema(self, elements, filters, default_value_func, element_label='dimension'):
        filters_schema = []
        for filter_item in filters:
            if isinstance(filter_item.element_key, (tuple, list)):
                element_key, modifier = filter_item.element_key
            else:
                element_key, modifier = filter_item.element_key, None

            element = elements.get(element_key)
            if not element:
                raise SlicerException(
                    'Unable to apply filter [{filter}].  '
                    'No such {element} with key [{key}].'.format(
                        filter=filter_item,
                        element=element_label,
                        key=filter_item.element_key
                    ))

            if hasattr(element, 'display_field') and 'display' == modifier:
                definition = element.display_field

            else:
                definition = element.definition or default_value_func(element.key)

            filters_schema.append(filter_item.schemas(definition))

        return filters_schema

    def _display_dimensions(self, dimensions, operations):
        req_dimension_keys = [utils.slice_first(dimension)
                              for dimension in dimensions]

        display_dims = OrderedDict()
        for key in req_dimension_keys:
            dimension = self.slicer.dimensions[key]
            display_dim = {'label': dimension.label}

            if hasattr(dimension, 'display_options'):
                display_dim['display_options'] = {opt.key: opt.label
                                                  for opt in dimension.display_options}

            total_operations = [operation for operation in operations if isinstance(operation, Totals)]
            for total_operation in total_operations:
                if key in total_operation.dimension_keys:
                    display_dim['display_options'][total_operation.key] = total_operation.label

            if hasattr(dimension, 'display_field') and dimension.display_field:
                display_dim['display_field'] = '%s_display' % dimension.key

            display_dims[key] = display_dim

        return display_dims

    def _references_schema(self, references, dimensions):
        dimension_keys = {utils.slice_first(dimension) for dimension in dimensions}

        schema_references = OrderedDict()
        for reference in references:
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

            schema_references[reference.key] = {
                'dimension': reference.element_key,
                'interval': reference.interval,
                'modifier': reference.modifier,
            }

        return schema_references

    def _default_dimension_definition(self, key):
        return fn.Coalesce(self.slicer.table.field(key), 'None')

    def _default_metric_definition(self, key):
        return fn.Sum(self.slicer.table.field(key))

    def _display_metrics(self, metrics, operations):
        display = OrderedDict()

        axis = 0
        for metrics_level in metrics:
            for metric_key in utils.wrap_list(metrics_level):
                schema = self.slicer.metrics[metric_key]
                display[metric_key] = {attr: getattr(schema, attr)
                                       for attr in ['label', 'precision', 'prefix', 'suffix']
                                       if getattr(schema, attr) is not None}
                display[metric_key]["axis"] = axis
            axis += 1

        for operation in operations:
            if not hasattr(operation, 'metric_key'):
                continue

            metric_key = operation.metric_key
            metric_schema = self.slicer.metrics[metric_key]

            key = '{}_{}'.format(metric_key, operation.key)
            display[key] = {attr: getattr(metric_schema, attr)
                            for attr in ['precision', 'prefix', 'suffix']
                            if getattr(metric_schema, attr) is not None}
            display[key]['label'] = '{} {}'.format(metric_schema.label, operation.label)
            display[key]["axis"] = axis
            axis += 1

        return display

    def _totals_schema(self, dimensions, operations):
        dimension_set = set(utils.slice_first(dimension) for dimension in dimensions)
        totals, missing_dimensions = [], set()
        for operation in operations:
            if operation.key != Totals.key:
                continue

            missing_dimensions |= set(operation.dimension_keys) - dimension_set
            totals += [level
                       for dimension in operation.dimension_keys
                       for level in self.slicer.dimensions[dimension].levels()]

        if missing_dimensions:
            raise SlicerException("Missing dimensions with keys: {}".format(", ".join(missing_dimensions)))

        return totals


class TransformerManager(object):
    def __init__(self, manager, transformers):
        self.manager = manager

        # Creates a function on the slicer for each transformer
        for tx_key, tx in transformers.items():
            setattr(self, tx_key, functools.partial(self._get_and_transform_data, tx))

    def _get_and_transform_data(self, tx, metrics=(), dimensions=(),
                                metric_filters=(), dimension_filters=(),
                                references=(), operations=()):
        """
        Handles a request and applies a transformation to the result.  This is the implementation of all of the
        transformer manager methods, which are constructed in the __init__ function of this class for each transformer.

        The request is first validated with the transformer then the request is executed via the SlicerManager and then
        lastly the result is transformed and returned.

        :param tx:
            The transformer to use
        :param metrics:
            See ``fireant.slicer.SlicerManager``
            A list of metrics to include in the query.

        :param dimensions:
            See ``fireant.slicer.SlicerManager``
            A list of dimensions to include in the query.

        :param metric_filters:
            See ``fireant.slicer.SlicerManager``
            A list of metrics filters to apply to the query.

        :param dimension_filters:
            See ``fireant.slicer.SlicerManager``
            A list of dimension filters to apply to the query.

        :param references:
            See ``fireant.slicer.SlicerManager``
            A list of references to include in the query

        :param operations:
            See ``fireant.slicer.SlicerManager``
            A list of post-operations to apply to the result before transformation.

        :return:
            The transformed result of the request.
        """
        tx.prevalidate_request(self.manager.slicer, metrics=metrics, dimensions=[utils.slice_first(dimension)
                                                                                 for dimension in dimensions],
                               metric_filters=metric_filters, dimension_filters=dimension_filters,
                               references=references, operations=operations)

        # Loads data and transforms it with a given transformer.
        dataframe = self.manager.data(metrics=utils.flatten(metrics), dimensions=dimensions,
                                      metric_filters=metric_filters, dimension_filters=dimension_filters,
                                      references=references, operations=operations)
        display_schema = self.manager.display_schema(metrics, dimensions, references, operations)

        return tx.transform(dataframe, display_schema)
