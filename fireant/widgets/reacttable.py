import re
from collections import OrderedDict

import pandas as pd
from fireant.dataset.fields import (
    DataType,
    Field,
)
from fireant.dataset.totals import (
    DATE_TOTALS,
    NUMBER_TOTALS,
    TEXT_TOTALS,
    TOTALS_MARKERS,
)
from fireant.formats import (
    RAW_VALUE,
    TOTALS_VALUE,
    display_value,
    json_value,
    raw_value,
    return_none,
    safe_value,
)
from fireant.reference_helpers import reference_alias
from fireant.utils import (
    alias_for_alias_selector,
    alias_selector,
    setdeepattr,
    wrap_list,
)

from .base import ReferenceItem
from .pandas import Pandas

TOTALS_LABEL = 'Totals'
METRICS_DIMENSION_ALIAS = 'metrics'
METRICS_DIMENSION = Field(METRICS_DIMENSION_ALIAS, None, data_type=DataType.text, label='')
F_METRICS_DIMENSION_ALIAS = alias_selector(METRICS_DIMENSION.alias)


def map_index_level(index, level, func):
    # If the index is empty, do not do anything
    if 0 == index.size:
        return index

    if isinstance(index, pd.MultiIndex):
        values = index.levels[level]
        return index.set_levels(values.map(func), level)

    assert level == 0

    return index.map(func)


class TotalsItem:
    alias = TOTALS_VALUE
    label = TOTALS_LABEL
    prefix = suffix = precision = None


class ReactTable(Pandas):
    """
    This component does not work with react-table out of the box, some customization is needed in order to work with
    the transformed data.

    .. code-block:: jsx

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
    """

    def __init__(self, metric, *metrics: Field, pivot=(), transpose=False, sort=None, ascending=None,
                 max_columns=None):
        super(ReactTable, self).__init__(metric, *metrics,
                                         pivot=pivot,
                                         transpose=transpose,
                                         sort=sort,
                                         ascending=ascending,
                                         max_columns=max_columns)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               ','.join(str(m) for m in self.items))

    @staticmethod
    def map_hyperlink_templates(df, dimensions):
        """
        Creates a mapping for each dimension to it's hyperlink template if it is possible to create the hyperlink
        template for it.

        The hyperlink template is a URL-like string containing curley braces enclosing dimension keys: `{dimension}`.
        While rendering this widget, the dimension key placeholders need to be replaced with the dimension values for
        that row.

        :param df:
            The result data set that is being transformed. The data frame SHOULD be pivoted/transposed if that step is
            required, before calling this function, in order to prevent the template from being included for the
            dimension if one of the required dimensions is pivoted.
        :param dimensions:
            The list of dimensions included in the query that created the result data set df.
        :return:
            A dict with the dimension key as the key and the hyperlink template as the value. Templates will only be
            included if it will be possible to fill in the required parameters.
        """
        hyperlink_templates = {}
        pattern = re.compile(r'{[^{}]+}')

        for dimension in dimensions:
            hyperlink_template = dimension.hyperlink_template
            if hyperlink_template is None:
                continue

            required_hyperlink_parameters = [alias_selector(argument[1:-1])
                                             for argument in pattern.findall(hyperlink_template)]

            # Check that all of the required dimensions are in the result data set. Only include the hyperlink template
            # in the return value of this function if all are present.
            unavailable_hyperlink_parameters = set(required_hyperlink_parameters) & set(df.index.names)
            if not unavailable_hyperlink_parameters:
                continue

            # replace the dimension keys with the formatted values. This will come in handy later when replacing the
            # actual values
            hyperlink_template = hyperlink_template.format(**{
                alias_for_alias_selector(argument): '{' + argument + '}'
                for argument in required_hyperlink_parameters
            })

            f_dimension_alias = alias_selector(dimension.alias)
            hyperlink_templates[f_dimension_alias] = hyperlink_template

        return hyperlink_templates

    @staticmethod
    def format_data_frame(data_frame):
        """
        This function prepares the raw data frame for transformation by formatting dates in the index and removing any
        remaining NaN/NaT values. It also names the column as metrics so that it can be treated like a dimension level.

        :param data_frame:
            The result set data frame
        :param dimensions:
        :return:
        """
        data_frame = data_frame.copy()
        data_frame.columns.name = F_METRICS_DIMENSION_ALIAS
        return data_frame

    @staticmethod
    def transform_index_column_headers(data_frame, field_map):
        """
        Convert the un-pivoted dimensions into ReactTable column header definitions.

        :param data_frame:
            The result set data frame
        :param field_map:
        :return:
            A list of column header definitions with the following structure.

        .. code-block:: jsx

            columns = [{
              Header: 'Column A',
              accessor: 'a',
            }, {
              Header: 'Column B',
              accessor: 'b',
            }]
        """
        columns = []
        if not isinstance(data_frame.index, pd.MultiIndex) and data_frame.index.name is None:
            return columns

        for f_dimension_alias in data_frame.index.names:
            dimension = field_map[f_dimension_alias]
            header = getattr(dimension, 'label', dimension.alias)

            columns.append({
                'Header': json_value(header),
                'accessor': safe_value(f_dimension_alias),
            })

        return columns

    @staticmethod
    def transform_data_column_headers(data_frame, field_map):
        """
        Convert the metrics into ReactTable column header definitions. This includes any pivoted dimensions, which will
        result in multiple rows of headers.

        :return:
        :param data_frame:
            The result set data frame
        :param field_map:
            A map to find metrics/operations based on their keys found in the data frame.
        :return:
            A list of column header definitions with the following structure.

        .. code-block:: jsx

            columns = [{
              Header: 'Column A',
              columns: [{
                Header: 'SubColumn A.0',
                accessor: 'a.0',
              }, {
                Header: 'SubColumn A.1',
                accessor: 'a.1',
              }]
            }, {
              Header: 'Column B',
              columns: [
                ...
              ]
            }]
        """

        def get_header(column_value, f_dimension_alias, is_totals):
            if f_dimension_alias == F_METRICS_DIMENSION_ALIAS or is_totals:
                item = field_map[column_value]
                return getattr(item, 'label', item.alias)

            if f_dimension_alias in field_map:
                field = field_map[f_dimension_alias]
                return display_value(column_value, field) or column_value

            return column_value

        def _make_columns(columns_frame, previous_level_values=()):
            """
            This function recursively creates the individual column definitions for React Table with the above tree
            structure depending on how many index levels there are in the columns.

            :param columns_frame:
                A data frame representing the columns of the result set data frame.
            :param previous_level_values:
                A tuple containing the higher level index level values used for building the data accessor path
            """
            f_dimension_alias = columns_frame.index.names[0]

            # Group the columns if they are multi-index so we can get the proper sub-column values. This will yield
            # one group per dimension value with the group data frame containing only the relevant sub-columns
            groups = columns_frame.groupby(level=0) \
                if isinstance(columns_frame.index, pd.MultiIndex) else \
                [(level, None) for level in columns_frame.index]

            columns = []
            for column_value, group in groups:
                is_totals = column_value in TOTALS_MARKERS | {TOTALS_LABEL}

                # All column definitions have a header
                column = {'Header': get_header(column_value, f_dimension_alias, is_totals)}

                level_values = previous_level_values + (column_value,)
                if group is not None:
                    # If there is a group, then drop this index level from the group data frame and recurse to build
                    # sub column definitions
                    next_level_df = group.reset_index(level=0, drop=True)
                    column['columns'] = _make_columns(next_level_df, level_values)

                else:
                    column['accessor'] = '.'.join(safe_value(value)
                                                  for value in level_values)

                if is_totals:
                    column['className'] = 'fireant-totals'

                columns.append(column)

            return columns

        # If the query only has a single metric, that level will be dropped, and set as data_frame.name
        dropped_metric_level_name = (data_frame.name,) if hasattr(data_frame, 'name') else ()

        return _make_columns(data_frame.columns.to_frame(),
                             dropped_metric_level_name)

    @staticmethod
    def transform_row_index(index_values, field_map, dimension_hyperlink_templates):
        # Add the index to the row
        row = {}
        for key, value in index_values.items():
            if key is None:
                continue

            field_alias = key
            field = METRICS_DIMENSION \
                if field_alias == METRICS_DIMENSION_ALIAS \
                else field_map[field_alias]

            data = {RAW_VALUE: raw_value(value, field)}
            display = display_value(value, field)
            if display is not None:
                data['display'] = display

            # If the dimension has a hyperlink template, then apply the template by formatting it with the dimension
            # values for this row. The values contained in `index_values` will always contain all of the required values
            # at this point, otherwise the hyperlink template will not be included.
            if key in dimension_hyperlink_templates:
                data['hyperlink'] = dimension_hyperlink_templates[key].format(**index_values)

            safe_key = safe_value(key)
            row[safe_key] = data

        return row

    @staticmethod
    def transform_row_values(series, fields, is_transposed):
        # Add the values to the row
        row = {}
        for key, value in series.iteritems():
            key = wrap_list(key)

            # Get the field for the metric
            metric_alias = (wrap_list(series.name)[0]
                            if is_transposed
                            else key[0])
            field = fields[metric_alias]

            data = {RAW_VALUE: raw_value(value, field)}
            display = display_value(value, field, date_as=return_none)
            if display is not None:
                data['display'] = display

            accessor_fields = [fields[field_alias]
                               for field_alias in series.index.names]
            accessor = [safe_value(value)
                        for value, field in zip(key, accessor_fields)]
            setdeepattr(row, accessor, data)

        return row

    @staticmethod
    def transform_data(data_frame, field_map, dimension_hyperlink_templates, is_transposed):
        """
        Builds a list of dicts containing the data for ReactTable. This aligns with the accessors set by
        #transform_dimension_column_headers and #transform_metric_column_headers

        :param data_frame:
            The result set data frame
        :param field_map:
            A mapping to all the fields in the slicer used for this query.
        :param dimension_hyperlink_templates:
        :param is_transposed:
        """
        index_names = data_frame.index.names

        def _get_field_label(alias):
            if alias not in field_map:
                return alias

            field = field_map[alias]
            return getattr(field, 'label', field.alias)

        # If the metric column was dropped due to only having a single metric, add it back here so the
        # formatting can be applied.
        if hasattr(data_frame, 'name'):
            metric_alias = data_frame.name
            data_frame = pd.concat([data_frame],
                                   keys=[metric_alias],
                                   names=[F_METRICS_DIMENSION_ALIAS],
                                   axis=1)

        rows = []
        for index, series in data_frame.iterrows():
            index = wrap_list(index)

            # Get a list of values from the index. These can be metrics or dimensions so it checks in the item map if
            # there is a display value for the value
            index_values = [_get_field_label(value) for value in index] \
                if is_transposed \
                else index
            index_display_values = OrderedDict(zip(index_names, index_values))

            rows.append({
                **ReactTable.transform_row_index(index_display_values,
                                                 field_map,
                                                 dimension_hyperlink_templates),
                **ReactTable.transform_row_values(series,
                                                  field_map,
                                                  is_transposed),
            })

        return rows

    def transform(self, data_frame, slicer, dimensions, references):
        """
        Transforms a data frame into a format for ReactTable. This is an object containing attributes `columns` and
        `data` which align with the props in ReactTable with the same name.

        :param data_frame:
            The result set data frame
        :param slicer:
            The slicer that generated the data query
        :param dimensions:
            A list of dimensions that were selected in the data query
        :param references:
            A list of references that were selected in the data query
        :return:
            An dict containing attributes `columns` and `data` which align with the props in ReactTable with the same
            names.
        """
        metric_map = OrderedDict([
            (
                alias_selector(reference_alias(item, ref)),
                ReferenceItem(item, ref) if ref is not None else item
            )
            for item in self.items
            for ref in [None] + references])
        dimension_map = {alias_selector(dimension.alias): dimension
                         for dimension in dimensions}
        field_map = {
            **metric_map,
            **dimension_map,

            # Add an extra item to map the totals markers to it's label
            NUMBER_TOTALS: TotalsItem,
            TEXT_TOTALS: TotalsItem,
            DATE_TOTALS: TotalsItem,
            TOTALS_LABEL: TotalsItem,
            alias_selector(METRICS_DIMENSION_ALIAS): METRICS_DIMENSION
        }
        all_dimensions_pivoted = 0 < len(dimensions) == len(self.pivot)  # has at least 1 dim and all are pivoted

        metric_aliases = list(metric_map.keys())
        dimension_aliases = [alias_selector(dimension.alias)
                             for dimension in self.pivot]

        df = self.format_data_frame(data_frame[metric_aliases])
        df = self.pivot_data_frame(df, dimension_aliases, self.transpose)

        dimension_columns = self.transform_index_column_headers(df, field_map)
        metric_columns = self.transform_data_column_headers(df, field_map)
        data = self.transform_data(df,
                                   field_map,
                                   is_transposed=self.transpose ^ all_dimensions_pivoted,
                                   dimension_hyperlink_templates=self.map_hyperlink_templates(df, dimensions))

        return {
            'columns': dimension_columns + metric_columns,
            'data': data,
        }
