# coding: utf-8
from datetime import time

import numpy as np
import pandas as pd

from fireant.slicer.transformers import Transformer

NO_TIME = time(0)


def _format_data_point(value):
    if isinstance(value, str):
        return str(value)

    if isinstance(value, pd.Timestamp):
        if value.time() == NO_TIME:
            return value.strftime('%Y-%m-%d')
        else:
            return value.strftime('%Y-%m-%dT%H:%M:%S')

    if value is None or np.isnan(value):
        return None

    if isinstance(value, np.int64):
        # Cannot transform np.int64 to json
        return int(value)

    return value


class DataTablesRowIndexTransformer(Transformer):
    def transform(self, dataframe, display_schema):
        has_references = isinstance(dataframe.columns, pd.MultiIndex)
        dataframe = self._prepare_dataframe(dataframe, display_schema['dimensions'])

        return {
            'columns': self._render_columns(dataframe, display_schema),
            'data': self._render_data(dataframe, display_schema),
        }

    def _prepare_dataframe(self, dataframe, dimensions):
        # Replaces invalid values and unstacks the data frame for column_index tables.
        return dataframe.replace([np.inf, -np.inf], np.nan)

    def _render_columns(self, dataframe, display_schema):
        dimensions = display_schema['dimensions']
        dimension_columns = [{'title': dimensions[dimension_key]['label'],
                              'data': '{}.display'.format(dimension_key)}
                             for dimension_key in dataframe.index.names
                             if dimension_key in dimensions]

        metric_columns = [self._render_column_level(metric_column, display_schema)
                          for metric_column in dataframe.columns]

        return dimension_columns + metric_columns

    def _render_column_level(self, metric_column, display_schema):
        if not isinstance(metric_column, tuple):
            return {'title': display_schema['metrics'][metric_column],
                    'data': metric_column}

        metric_key_idx = 1 if 'references' in display_schema else 0
        if metric_key_idx and metric_column[0]:
            reference_key = metric_column[0]
            metric_key = metric_column[1]
            data_keys = [reference_key, metric_key]
            metric_label = '{metric} {reference}'.format(
                metric=display_schema['metrics'][metric_key],
                reference=display_schema['references'][reference_key]
            )

        else:
            metric_key = metric_column[metric_key_idx]
            data_keys = [metric_key]
            metric_label = display_schema['metrics'][metric_key]

        return {
            'title': metric_label,
            'data': '.'.join(map(str, data_keys))
        }

    def _render_data(self, dataframe, display_schema):
        n = len(dataframe.index.levels) if isinstance(dataframe.index, pd.MultiIndex) else 1
        dimensions = list(display_schema['dimensions'].items())
        row_dimensions, column_dimensions = dimensions[:n], dimensions[n:]

        data = []
        for idx, df_row in dataframe.iterrows():
            row = {}

            if not isinstance(idx, tuple):
                idx = (idx,)

            for key, value in self._render_dimension_data(idx, row_dimensions):
                row[key] = value

            for key, value in self._render_metric_data(df_row, column_dimensions,
                                                       display_schema['metrics'], display_schema.get('references')):
                row[key] = value

            data.append(row)

        return data

    def _render_dimension_data(self, idx, dimensions):
        i = 0
        for key, schema in dimensions:
            dimension_value = _format_data_point(idx[i])

            if 'label_field' in schema:
                i += 1
                yield key, {'display': dimension_value,
                            'value': _format_data_point(idx[i])}

            elif 'label_options' in schema:
                yield key, {'display': schema['label_options'].get(dimension_value, dimension_value) or 'Total',
                            'value': dimension_value}

            else:
                yield key, {'display': dimension_value}

            i += 1

    def _render_metric_data(self, df, dimensions, metrics, references):
        if not isinstance(df.index, pd.MultiIndex):
            for metric_key, label in metrics.items():
                yield metric_key, _format_data_point(df[metric_key])

        if references:
            for reference in [''] + list(references):
                for key, value in self._recurse_dimensions(df[reference], dimensions, metrics, reference or None):
                    yield key, value
            return

        for key, value in self._recurse_dimensions(df, dimensions, metrics):
            yield key, value

    def _recurse_dimensions(self, df, dimensions, metrics, reference=None):
        if reference:
            return [(reference, dict(self._recurse_dimensions(df, dimensions, metrics)))]

        return [(metric_key, _format_data_point(df[metric_key]))
                for metric_key, label in metrics.items()]


class DataTablesColumnIndexTransformer(DataTablesRowIndexTransformer):
    def _prepare_dataframe(self, dataframe, dimensions):
        # Replaces invalid values and unstacks the data frame for column_index tables.
        dataframe = super(DataTablesColumnIndexTransformer, self)._prepare_dataframe(dataframe, dimensions)

        if 2 > len(dimensions):
            return dataframe

        unstack_levels = []
        for key, dimension in list(dimensions.items())[1:]:
            unstack_levels.append(key)
            if 'label_field' in dimension:
                unstack_levels.append(dimension['label_field'])

        return dataframe.unstack(level=unstack_levels)

    def _render_column_level(self, metric_column, display_schema):
        column = super(DataTablesColumnIndexTransformer, self)._render_column_level(metric_column, display_schema)

        # Iterate through the pivoted levels
        i = 2 if 'references' in display_schema else 1
        data_keys, levels = [], []
        for dimension_key, dimension in list(display_schema['dimensions'].items())[1:]:
            if 'label_options' in dimension:
                level_key = metric_column[i]
                level_label = (dimension['label_options'].get(level_key, None)
                               if not (isinstance(level_key, float) and np.isnan(level_key))
                               else 'Total')

            elif 'label_field' in dimension:
                level_key = metric_column[i]
                i += 1
                level_label = metric_column[i]

            # the metric key must remain last
            if not (isinstance(level_key, float) and np.isnan(level_key)):
                levels.append(level_label)
                data_keys.append(level_key)

            i += 1

        if levels:
            metric_label = '{metric} ({levels})'.format(
                metric=column['title'],
                levels=', '.join(levels)
            )
        else:
            metric_label = column['title']

        if data_keys and '.' in column['data']:
            # Squeeze the dimension levels between the reference and the metric
            data = column['data'].replace('.', '.' + ('.'.join(map(str, data_keys))) + '.')
        else:
            # Prepend the dimension levels to the metric
            data = '.'.join(map(str, data_keys + [column['data']]))

        return {
            'title': metric_label,
            'data': data
        }

    def _recurse_dimensions(self, df, dimensions, metrics, reference=None):
        if reference or not dimensions:
            return super(DataTablesColumnIndexTransformer, self)._recurse_dimensions(df, dimensions, metrics, reference)

        if 'label_field' in dimensions[0][1]:
            return [(key, dict(self._recurse_dimensions(df[:, key, display], dimensions[1:], metrics)))
                    for key, display in zip(*df.index.levels[1:3])]

        return [(level, dict(self._recurse_dimensions(df[:, level], dimensions[1:], metrics)))
                for level in df.index.levels[1]]


class CSVRowIndexTransformer(DataTablesRowIndexTransformer):
    def transform(self, data_frame, display_schema):
        dim_ordinal = {name: ordinal
                       for ordinal, name in enumerate(data_frame.index.names)}

        csv_df = self._format_columns(data_frame, dim_ordinal, display_schema)

        if isinstance(data_frame.index, pd.RangeIndex):
            # If there are no dimensions, just serialize to csv without the index
            return csv_df.to_csv(index=False)

        csv_df = self._format_index(csv_df, dim_ordinal, display_schema)

        row_dimensions = display_schema['dimensions'][:None if self.table_type == 'row' else 1]
        return csv_df.to_csv(index_label=[dimension['label']
                                          for dimension in row_dimensions])

    def _format_index(self, csv_df, dim_ordinal, display_schema):
        if isinstance(csv_df.index, pd.MultiIndex):
            csv_df.index = pd.MultiIndex.from_tuples(
                [[self._format_dimension_label(idx, dim_ordinal, dimension)
                  for dimension in display_schema['dimensions']]
                 for idx in list(csv_df.index)]
            )

        else:
            csv_df.reindex(
                self._format_dimension_label(idx, dim_ordinal, display_schema['dimensions'][0])
                for idx in list(csv_df.index)
            )

        return csv_df

    def _format_columns(self, data_frame, dim_ordinal, display_schema):
        if 1 < len(display_schema['dimensions']) and self.table_type == 'column':
            csv_df = self._prepare_data_frame(data_frame, display_schema['dimensions'])

            csv_df.columns = pd.Index([self._format_series_labels(column, dim_ordinal, display_schema)
                                       for column in csv_df.columns])
            return csv_df

        return data_frame.rename(
            columns=lambda metric: display_schema['metrics'].get(metric, metric)
        )


class CSVColumnIndexTransformer(DataTablesColumnIndexTransformer, CSVRowIndexTransformer):
    pass
