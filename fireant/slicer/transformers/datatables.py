# coding: utf-8
import numpy as np
import pandas as pd

from datetime import time
from fireant.slicer.operations import Totals
from fireant import settings
from fireant.slicer.transformers import Transformer

NO_TIME = time(0)


def _safe(value):
    if isinstance(value, pd.Timestamp):
        if value.time() == NO_TIME:
            return value.strftime('%Y-%m-%d')
        else:
            return value.strftime('%Y-%m-%dT%H:%M:%S')

    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None

    if isinstance(value, np.int64):
        # Cannot transform np.int64 to json
        return int(value)

    return value


def _pretty(value, schema):
    if value and isinstance(value, float) and 'precision' in schema:
        value = round(value, schema['precision'])

    return '{prefix}{value}{suffix}'.format(
        prefix=schema.get('prefix', ''),
        value=str(value),
        suffix=schema.get('suffix', ''),
    )


def _format_value(value, metric):
    raw_value = _safe(value)
    return {'value': raw_value, 'display': _pretty(raw_value, metric) if raw_value is not None else None}


def _format_column_display(csv_df, metrics, dimensions):
    column_display = []
    for idx in list(csv_df.columns):
        metric_value, dimension_values = idx[0], idx[1:]

        dimension_displays = []
        for dimension_level, dimension_value in zip(csv_df.columns.names[1:], dimension_values):
            if 'display_options' in dimensions[dimension_level]:
                dimension_display = dimensions[dimension_level]['display_options'].get(dimension_value,
                                                                                       dimension_value)
            else:
                dimension_display = dimension_value

            dimension_display = _safe(dimension_display)

            if dimension_display != Totals.label:
                dimension_displays.append(dimension_display)

        if dimension_displays:
            column_display.append('{metric} ({dimensions})'.format(
                metric=metrics[metric_value]['label'],
                dimensions=', '.join(dimension_displays),
            ))
        else:
            column_display.append(metrics[metric_value]['label'])

    return column_display


class DataTablesRowIndexTransformer(Transformer):
    def transform(self, dataframe, display_schema):
        dataframe = self._prepare_dataframe(dataframe, display_schema['dimensions'])

        return {
            'columns': self._render_columns(dataframe, display_schema),
            'data': self._render_data(dataframe, display_schema),
        }

    def _prepare_dataframe(self, dataframe, dimensions):
        # Replaces invalid values and unstacks the data frame for column_index tables.
        return dataframe.replace([np.inf, -np.inf], np.nan)

    def _render_columns(self, dataframe, display_schema):
        maxcols = settings.datatables_maxcols
        dimension_columns = [self._render_dimension_column(dimension_key, display_schema)
                             for dimension_key in dataframe.index.names[:maxcols]
                             if dimension_key in display_schema['dimensions']]

        maxcols -= len(dimension_columns)
        metric_columns = [self._render_column_level(metric_column, display_schema)
                          for metric_column in list(dataframe.columns)[:maxcols]]

        return dimension_columns + metric_columns

    def _render_dimension_column(self, key, display_schema):
        dimension = display_schema['dimensions'][key]

        if 'display_field' in dimension or 'display_options' in dimension:
            render = {
                '_': 'display',
                'type': 'display',
                'sort': 'display'
            }
        else:
            render = {
                '_': 'value',
                'type': 'value',
                'sort': 'value'
            }

        return {'title': dimension['label'],
                'data': '{}'.format(key),
                'render': render}

    def _render_column_level(self, metric_column, display_schema):
        metrics = display_schema['metrics']
        if not isinstance(metric_column, tuple):
            return {'title': metrics[metric_column]['label'],
                    'data': metric_column,
                    'render': {'type': 'value', '_': 'display', 'sort': 'value'}}

        references = display_schema.get('references')
        metric_key_idx = 1 if references else 0
        if metric_key_idx and metric_column[0]:
            reference_key = metric_column[0]
            metric_key = metric_column[1]
            data_keys = [reference_key, metric_key]
            metric_label = '{metric} {reference}'.format(
                metric=metrics[metric_key]['label'],
                reference=display_schema['references'][reference_key]
            )

        else:
            metric_key = metric_column[metric_key_idx]
            data_keys = [metric_key]
            metric_label = metrics[metric_key]['label']

        path = '.'.join(map(str, data_keys))
        return {
            'title': metric_label,
            'data': path,
            'render': {'type': 'value', '_': 'display', 'sort': 'value'}
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
        for key, dimension in dimensions:
            dimension_value = _safe(idx[i])

            if 'display_field' in dimension:
                i += 1
                yield key, {'display': _safe(idx[i]), 'value': dimension_value}

            elif 'display_options' in dimension:
                display = dimension['display_options'].get(dimension_value, dimension_value)
                yield key, {'display': display, 'value': dimension_value}

            else:
                yield key, {'value': dimension_value}

            i += 1

    def _render_metric_data(self, dataframe, dimensions, metrics, references):
        if not isinstance(dataframe.index, pd.MultiIndex):
            for metric_key, label in metrics.items():
                yield metric_key, _format_value(dataframe[metric_key], metrics[metric_key])

        if references:
            for reference in [''] + list(references):
                for key, value in self._recurse_dimensions(dataframe[reference], dimensions, metrics,
                                                           reference or None):
                    yield key, value
            return

        for key, value in self._recurse_dimensions(dataframe, dimensions, metrics):
            yield key, value

    def _recurse_dimensions(self, dataframe, dimensions, metrics, reference=None):
        if reference:
            return [(reference, dict(self._recurse_dimensions(dataframe, dimensions, metrics)))]

        return [(metric_key, _format_value(dataframe[metric_key], metrics[metric_key]))
                for metric_key, label in metrics.items()]


class DataTablesColumnIndexTransformer(DataTablesRowIndexTransformer):
    def _prepare_dataframe(self, dataframe, dimensions):
        # Replaces invalid values and unstacks the data frame for column_index tables.
        dataframe = super(DataTablesColumnIndexTransformer, self)._prepare_dataframe(dataframe, dimensions)

        unstack_levels = []
        for key, dimension in list(dimensions.items())[1:]:
            unstack_levels.append(key)
            if 'display_field' in dimension:
                unstack_levels.append(dimension['display_field'])

        return dataframe.unstack(level=unstack_levels)

    def _render_column_level(self, metric_column, display_schema):
        column = super(DataTablesColumnIndexTransformer, self)._render_column_level(metric_column, display_schema)

        # Iterate through the pivoted levels
        i = 2 if display_schema.get('references') else 1
        data_keys, levels = [], []
        for dimension in list(display_schema['dimensions'].values())[1:]:
            if 'display_options' in dimension:
                level_key = metric_column[i]
                level_display = dimension['display_options'].get(level_key, level_key)

            else:
                level_key = metric_column[i]

                if 'display_field' in dimension:
                    i += 1

                level_display = metric_column[i]

            # the metric key must remain last
            if not (isinstance(level_key, float) and np.isnan(level_key)):
                if level_key != Totals.key:
                    levels.append(level_display)
                data_keys.append(level_key)

            i += 1

        if levels:
            metric_label = '{metric} ({levels})'.format(
                metric=column['title'],
                levels=', '.join(map(str, levels))
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
            'data': data,
            'render': {'type': 'value', '_': 'display', 'sort': 'value'}
        }

    def _recurse_dimensions(self, dataframe, dimensions, metrics, reference=None):
        if dataframe.empty:
            return []

        if reference or not dimensions:
            return super(DataTablesColumnIndexTransformer, self)._recurse_dimensions(dataframe, dimensions, metrics,
                                                                                     reference)

        data = []

        if 'display_field' in dimensions[0][1]:
            levels = zip(*dataframe.index.levels[1:3])
            format_key = lambda level: level[0]
            slice_metric_data = lambda level: dataframe[:, level[0], level[1]]
        else:
            levels = dataframe.index.levels[1]
            format_key = str
            slice_metric_data = lambda level: dataframe[:, level]

        for level in levels:
            metric_data =  dict(self._recurse_dimensions(slice_metric_data(level), dimensions[1:], metrics))

            if not metric_data:
                continue

            data.append((format_key(level), metric_data))

        return data



class CSVRowIndexTransformer(DataTablesRowIndexTransformer):
    def transform(self, dataframe, display_schema):
        csv_df = self._format_columns(dataframe, display_schema['metrics'], display_schema['dimensions'])

        if isinstance(dataframe.index, pd.RangeIndex):
            # If there are no dimensions, just serialize to csv without the index
            return csv_df.to_csv(index=False)

        csv_df = self._format_index(csv_df, display_schema['dimensions'])

        row_dimension_labels = self._format_row_dimension_labels(display_schema['dimensions'])
        return csv_df.to_csv(index_label=row_dimension_labels)

    def _format_index(self, csv_df, dimensions):
        levels = list(dimensions.items())[:None if isinstance(csv_df.index, pd.MultiIndex) else 1]

        csv_df.index = [self.get_level_values(csv_df, key, dimension)
                        for key, dimension in levels]
        csv_df.index.names = [key
                              for key, dimension in levels]
        return csv_df

    def get_level_values(self, csv_df, key, dimension):
        if 'display_options' in dimension:
            return [_safe(dimension['display_options'].get(value, value))
                    for value in csv_df.index.get_level_values(key)]

        if 'display_field' in dimension:
            return [_safe(data_point)
                    for data_point in csv_df.index.get_level_values(dimension['display_field'])]

        return [_safe(data_point)
                for data_point in csv_df.index.get_level_values(key)]

    @staticmethod
    def _format_dimension_label(idx, dim_ordinal, dimension):
        if 'display_field' in dimension:
            display_field = dimension['display_field']
            return idx[dim_ordinal[display_field]]

        if isinstance(idx, tuple):
            id_field = dimension['definition'][0]
            dimension_display = idx[dim_ordinal[id_field]]

        else:
            dimension_display = idx

        if 'display_options' in dimension:
            return dimension['display_options'].get(dimension_display, dimension_display)

        return dimension_display

    def _format_columns(self, dataframe, metrics, dimensions):
        return dataframe.rename(columns=lambda metric: metrics[metric].get('label', metric))

    def _format_row_dimension_labels(self, dimensions):
        return [dimension['label']
                for dimension in dimensions.values()]


class CSVColumnIndexTransformer(DataTablesColumnIndexTransformer, CSVRowIndexTransformer):
    def _format_columns(self, dataframe, metrics, dimensions):
        if 1 < len(dimensions):
            csv_df = self._prepare_dataframe(dataframe, dimensions)
            csv_df.columns = _format_column_display(csv_df, metrics, dimensions)
            return csv_df

        return super(CSVColumnIndexTransformer, self)._format_columns(dataframe, metrics, dimensions)

    def _format_row_dimension_labels(self, dimensions):
        return [dimension['label']
                for dimension in list(dimensions.values())[:1]]
