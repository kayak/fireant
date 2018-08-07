from collections import OrderedDict

import pandas as pd

from fireant.formats import (
    RAW_VALUE,
    TOTALS_VALUE,
)
from fireant.utils import (
    format_dimension_key,
    format_metric_key,
    getdeepattr,
    setdeepattr,
)
from .pandas import Pandas
from ..dimensions import (
    DatetimeDimension,
    Dimension,
)
from ..intervals import (
    annually,
    daily,
    hourly,
    monthly,
    quarterly,
    weekly,
)
from ..metrics import Metric
from ..references import (
    reference_key,
    reference_label,
)

DATE_FORMATS = {
    hourly: '%Y-%m-%d %h',
    daily: '%Y-%m-%d',
    weekly: '%Y-%m',
    monthly: '%Y',
    quarterly: '%Y-%q',
    annually: '%Y',
}

metrics = Dimension('metrics', '')
metrics_dimension_key = format_dimension_key(metrics.key)


def map_index_level(index, level, func):
    if isinstance(index, pd.MultiIndex):
        values = index.levels[level]
        return index.set_levels(values.map(func), level)

    assert level == 0

    return index.map(func)


def fillna_index(index, value):
    if isinstance(index, pd.MultiIndex):
        return pd.MultiIndex.from_tuples([[value if pd.isnull(x) else x
                                           for x in row]
                                          for row in index],
                                         names=index.names)

    return index.fillna(value)


def _get_item_display(item, value):
    if item is None or all([item.prefix is None, item.suffix is None, item.precision is None]):
        return

    return '{prefix}{value}{suffix}'.format(
          prefix=(item.prefix or ""),
          suffix=(item.suffix or ""),
          value=value if item.precision is None else '{:.{precision}f}'.format(0.1234567890, precision=2)
    )


def make_column_format(item):
    if item is None or all([item.prefix is None, item.suffix is None, item.precision is None]):
        return

    return '{prefix}{format}{suffix}'.format(
          prefix=(item.prefix or ""),
          suffix=(item.suffix or "").replace('%', '%%'),
          format='%s' if item.precision is None else '%.{}f'.format(item.precision)
    )


class ReferenceItem:
    def __init__(self, item, reference):
        if reference is None:
            self.key = item.key
            self.label = item.label
        else:
            self.key = reference_key(item, reference)
            self.label = reference_label(item, reference)

        self.prefix = item.prefix
        self.suffix = item.suffix
        self.precision = item.precision


class TotalsItem:
    key = TOTALS_VALUE
    label = 'Totals'
    prefix = suffix = precision = None


class ReactTable(Pandas):
    """
    This component does not work with react-table out of the box, some customization is needed in order to work with
    the transformed data.

    ```
    // A Custom TdComponent implementation is required by Fireant in order to render display values
    const TdComponent = ({
                           toggleSort,
                           className,
                           children,
                           ...rest
                         }) =>
        <div className={classNames('rt-td', className)} role="gridcell" {...rest}>
            {_.get(children, 'display', children.raw) || <span>&nbsp;</span>}
        </div>;

    const FireantReactTable = ({
                            config, // The payload from fireant
                          }) =>
        <ReactTable columns={config.columns}
                    data={config.data}
                    minRows={0}

                    TdComponent={ DashmoreTdComponent}
                    defaultSortMethod={(a, b, desc) => ReactTableDefaults.defaultSortMethod(a.raw, b.raw, desc)}>
        </ReactTable>;
    ```
    """

    def __init__(self, metric, *metrics: Metric, pivot=(), transpose=False, max_columns=None):
        super(ReactTable, self).__init__(metric, *metrics,
                                         pivot=pivot,
                                         transpose=transpose,
                                         max_columns=max_columns)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               ','.join(str(m) for m in self.items))

    def map_display_values(self, df, dimensions):
        """
        WRITEME

        :param df:
        :param dimensions:
        :return:
        """
        dimension_display_values = {}

        for dimension in dimensions:
            f_dimension_key = format_dimension_key(dimension.key)

            if dimension.has_display_field:
                f_display_key = format_dimension_key(dimension.display_key)

                dimension_display_values[f_dimension_key] = \
                    df[f_display_key].groupby(f_dimension_key).first().fillna('null').to_dict()

                del df[f_display_key]

            if hasattr(dimension, 'display_values'):
                dimension_display_values[f_dimension_key] = dimension.display_values

        return dimension_display_values

    @staticmethod
    def format_data_frame(df, dimensions):
        """
        This function prepares the raw data frame for transformation by formatting dates in the index and removing any
        remaining NaN/NaT values. It also names the column as metrics so that it can be treated like a dimension level.

        :param df:
        :param dimensions:
        :return:
        """
        for i, dimension in enumerate(dimensions):
            if isinstance(dimension, DatetimeDimension):
                date_format = DATE_FORMATS.get(dimension.interval, DATE_FORMATS[daily])
                df.index = map_index_level(df.index, i, lambda dt: dt.strftime(date_format))

        df.index = fillna_index(df.index, TOTALS_VALUE)
        df.columns.name = metrics_dimension_key

    @staticmethod
    def transform_dimension_column_headers(data_frame, dimensions):
        """
        Convert the unpivoted dimensions into ReactTable column header definitions.

        :param data_frame:
        :param dimensions:
        :return:
        """
        dimension_map = {format_dimension_key(d.key): d
                         for d in dimensions + [metrics]}

        columns = []
        if isinstance(data_frame.index, pd.RangeIndex):
            return columns

        for f_dimension_key in data_frame.index.names:
            dimension = dimension_map[f_dimension_key]

            columns.append({
                'Header': getattr(dimension, 'label', dimension.key),
                'accessor': f_dimension_key,
            })

        return columns

    @staticmethod
    def transform_metric_column_headers(data_frame, item_map, dimension_display_values):
        """
        Convert the metrics into ReactTable column header definitions. This includes any pivoted dimensions, which will
        result in multiple rows of headers.

        :param data_frame:
        :param item_map:
        :param dimension_display_values:
        :return:
        """

        def get_header(column_value, f_dimension_key, is_totals):
            if f_dimension_key == metrics_dimension_key or is_totals:
                item = item_map[column_value]
                return getattr(item, 'label', item.key)

            else:
                return getdeepattr(dimension_display_values, (f_dimension_key, column_value), column_value)

        def _make_columns(df, previous_levels=()):
            f_dimension_key = df.index.names[0]
            groups = df.groupby(level=0) \
                if isinstance(df.index, pd.MultiIndex) else \
                [(level, None) for level in df.index]

            columns = []
            for column_value, group in groups:
                is_totals = TOTALS_VALUE == column_value

                column = {'Header': get_header(column_value, f_dimension_key, is_totals)}

                levels = previous_levels + (column_value,)
                if group is not None:
                    next_level_df = group.reset_index(level=0, drop=True)
                    column['columns'] = _make_columns(next_level_df, levels)

                else:
                    if hasattr(data_frame, 'name'):
                        levels += (data_frame.name,)
                    column['accessor'] = '.'.join(levels)

                if is_totals:
                    column['className'] = 'fireant-totals'

                columns.append(column)

            return columns

        column_frame = data_frame.columns.to_frame()
        return _make_columns(column_frame)

    @staticmethod
    def transform_data(data_frame, item_map, dimension_display_values):
        """
        WRITEME

        :param data_frame:
        :param item_map:
        :param dimension_display_values:
        :return:
        """
        result = []

        for index, series in data_frame.iterrows():
            if not isinstance(index, tuple):
                index = (index,)

            index = [x
                     if x not in item_map
                     else getattr(item_map[x], 'label', item_map[x].key)
                     for x in index]

            row = {}
            for key, value in zip(data_frame.index.names, index):
                if key is None:
                    continue

                data = {RAW_VALUE: value}
                display = getdeepattr(dimension_display_values, (key, value))
                if display is not None:
                    data['display'] = display

                row[key] = data

            for key, value in series.iteritems():
                data = {RAW_VALUE: value}
                item = item_map.get(key[0] if isinstance(key, tuple) else key)
                display = _get_item_display(item, value)
                if display is not None:
                    data['display'] = display

                setdeepattr(row, key, data)

            result.append(row)

        return result

    def transform(self, data_frame, slicer, dimensions, references):
        """
        WRITEME

        :param data_frame:
        :param slicer:
        :param dimensions:
        :param references:
        :return:
        """
        df_dimension_columns = [format_dimension_key(d.display_key)
                                for d in dimensions
                                if d.has_display_field]
        item_map = OrderedDict([(format_metric_key(reference_key(i, reference)), ReferenceItem(i, reference))
                                for i in self.items
                                for reference in [None] + references])
        df_metric_columns = list(item_map.keys())

        # Add an extra item to map the totals key to it's label
        item_map[TOTALS_VALUE] = TotalsItem

        df = data_frame[df_dimension_columns + df_metric_columns].copy()

        dimension_display_values = self.map_display_values(df, dimensions)
        self.format_data_frame(df, dimensions)

        dimension_keys = [format_dimension_key(dimension.key) for dimension in self.pivot]
        df = self.pivot_data_frame(df, dimension_keys, self.transpose)

        dimension_columns = self.transform_dimension_column_headers(df, dimensions)
        metric_columns = self.transform_metric_column_headers(df, item_map, dimension_display_values)
        data = self.transform_data(df, item_map, dimension_display_values)

        return {
            'columns': dimension_columns + metric_columns,
            'data': data,
        }
