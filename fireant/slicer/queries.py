# coding: utf-8
import copy
import logging
import time
from itertools import chain

import pandas as pd
from pypika import JoinType, MySQLQuery

from fireant import utils
from fireant.slicer.references import (YoY,
                                       Delta,
                                       DeltaPercentage)

query_logger = logging.getLogger('fireant.query_log$')


class QueryNotSupportedError(Exception):
    pass


class QueryManager(object):
    def __init__(self, database):
        # Get the correct pypika database query class
        self.query_cls = database.query_cls

    def query_data(self, database, table, joins=None,
                   metrics=None, dimensions=None,
                   mfilters=None, dfilters=None,
                   references=None, rollup=None):
        """
        Loads a pandas data frame given a table and a description of the request.

        :param database:
            The database interface to use to execute the connection

       :param table:
            Type: pypika.Table
            (Required) The primary table to select data from.  In SQL, this is the table in the FROM clause.

        :param joins:
            Type: list[tuple[pypika.Table, pypika.Criterion, pypika.JoinType]]
            (Optional) A list of tables to join in the query.  If a metric should be selected from a table besides the
            table parameter, the additional table *must* be joined.

            The tuple includes in the following order: Join table, Join criterion, Join type

        :param metrics:
            Type: dict[str: pypika.Field]]
            A dict containing metrics fields to query.  This value is required to contain at least one entry.  The key
            used will match the corresponding column in the returned pd.DataFrame.

        :param dimensions:
            Type: OrderedDict[str: pypika.Field]]
            (Optional) An OrderedDict containing indices to query.  If empty, a pd.Series will be returned containing
            one value per metric.  If given one entry, a pd.DataFrame with a single index will be returned.  If more
            than one value is given, then a pd.DataFrame with a multi-index will be returned.  The key used will match
            the corresponding index level in the returned DataFrame.

            If a dict is used instead of an OrderedDict, then the index levels cannot be guaranteed, so in most cases an
            OrderedDict should be used.

        :param mfilters:
            Type: list[pypika.Criterion]
            (Optional) A list containing criterions to use in the HAVING clause of the query.  This will reduce the data
            to instances where metrics meet the criteria.

        :param dfilters:
            Type: list[pypika.Criterion]
            (Optional) A list containing criterions to use in the WHERE clause of the query.  This will reduce the data
            to instances where dimensions meet the criteria.

        :param references:
            Type: dict[str: str]]
            (Optional) A dict containing comparison operations.  The key must match a valid reference.  The value must
            match the key of a provided dimension.  The value must match a dimensions of the appropriate type, usually a
            Time Series dimension.

            For each value passed, the query will be duplicated containing values for a comparison.

            References can also modify the comparison field to show values such as change.  The format is as follows:

            {key}_{modifier}

            The modifier type is optional and the underscore must be omitted when one is not used.

            Valid references are as follows:

            - *wow*: Week over Week - duplicates the query for values one week prior.
            - *mom*: Month over Month - duplicates the query for values 4 weeks prior.
            - *qoq*: Quarter over Quarter - duplicates the query for values one quarter prior.
            - *yoy*: Year over Year - duplicates the query for values 52 weeks prior.

            Valid modifier types are as follows:

            - *d*: Delta - difference between previous value and current
            - *p*: Delta Percentage - difference between previous value and current as a percentage of the previous
                    value.

            Examples:

            "wow":   Week over Week
            "wow_d": delta Week over Week

        :param rollup:
            Type: list[str]
            (Optional) Lists the dimensions to include for ROLLUP which calculates the totals across groups.  This list
            must be a subset of the keys of the dimensions parameter dict.

            .. note::

                When using rollup for less than all of the dimensions, the dimensions included in the ROLLUP will be
                moved after the non-ROLLUP dimensions.

        :return:
            A pd.DataFrame indexed by the provided dimensions parameters containing columns for each metrics parameter.
        """
        if rollup and database.query_cls is MySQLQuery:
            # MySQL doesn't currently support query rollups in the same way as Vertica, Oracle etc.
            # We therefore don't support it for now.
            raise QueryNotSupportedError("MySQL currently doesn't support ROLLUP operations!")

        query = self._build_data_query(
            database, table, joins, metrics, dimensions, dfilters, mfilters, references, rollup
        )
        query_string = str(query)

        start_time = time.time()
        dataframe = database.fetch_dataframe(query_string)

        query_logger.info('[duration: {duration} seconds]: {query}'.format(
            duration=round(time.time() - start_time, 4),
            query=query_string)
        )

        dataframe.columns = [col.decode('utf-8') if isinstance(col, bytes) else col
                             for col in dataframe.columns]

        for column in dataframe.columns:
            # Only replace NaNs for columns of type object. Column types other than that tend to be checked
            # against in the transformers. Which would be a problem when replacing NaNs with a string
            # because that alters the type of the column.
            if dataframe[column].dtype == object:
                dataframe[column] = dataframe[column].fillna('')

        if dimensions:
            dataframe = dataframe.set_index(
                # Removed the reference keys for now
                list(dimensions.keys())  # + ['{1}_{0}'.format(*ref) for ref in references.items()]
            ).sort_index()

        if references:
            dataframe.columns = pd.MultiIndex.from_product([[''] + list(references.keys()), list(metrics.keys())])

        return dataframe.fillna(0)

    def query_dimension_options(self, database, table, joins=None, dimensions=None, filters=None, limit=None):
        """
        Builds and executes a query to retrieve possible dimension options given a set of filters.

        :param database:
            The database interface to use to execute the connection
        :param table:
            See above

        :param joins:
            See above

        :param dimensions:
            See above

        :param filters:
            See above

        :param limit:
            An optional limit to the number of results returned.

        :return:
        """
        query = self._build_dimension_query(table, joins, dimensions, filters, limit)
        results = database.fetch(str(query))
        return [{k: v for k, v in zip(dimensions.keys(), result)}
                for result in results]

    def _build_data_query(self, database, table, joins, metrics, dimensions, dfilters, mfilters, references, rollup):
        args = (table, joins or dict(), metrics or dict(), dimensions or dict(),
                dfilters or dict(), mfilters or dict(), rollup or list())

        query = self._build_query_inner(*args)

        if references:
            return self._build_reference_query(query, database, references, *args)

        return self._add_sorting(query, list(dimensions.values()))

    def _build_reference_query(self, query, database, references, table, joins, metrics, dimensions, dfilters,
                               mfilters, rollup):
        wrapper_query = self.query_cls.from_(query).select(*[query.field(key).as_(key)
                                                             for key in list(dimensions.keys()) + list(metrics.keys())])

        for reference_key, schema in references.items():
            dimension_key, interval = schema['dimension'], schema['interval']

            # The interval term from pypika does not take into account leap years, therefore the interval
            # needs to be replaced with a database specific one when appropriate.
            if reference_key in [YoY.key, Delta.generate_key(YoY.key), DeltaPercentage.generate_key(YoY.key)]:
                interval = database.interval(years=1)

            schema['interval'] = interval

            # Don't reuse the dfilters arg otherwise intervals will be aggregated on each iteration
            replaced_dfilters = self._replace_filters_for_ref(dfilters, schema['definition'], interval)
            ref_query = self._build_query_inner(table, joins, metrics, dimensions, replaced_dfilters, mfilters, rollup)
            join_criteria = self._build_reference_join_criteria(dimension_key, dimensions, interval, query, ref_query)

            # Optional modifier function to modify the metric in the reference query. This is for delta and delta
            # percentage references. It is None for normal references and this default should be used
            modifier = schema.get('modifier')

            # Join the reference query and select the reference dimension and all metrics
            # This ignores additional dimensions since they are identical to the primary results
            if join_criteria:
                wrapper_query = wrapper_query.join(ref_query, JoinType.left).on(join_criteria)
            else:
                wrapper_query = wrapper_query.from_(ref_query)

            if modifier:
                get_reference_field = lambda key: modifier(query.field(key), ref_query.field(key))

            else:
                get_reference_field = lambda key: ref_query.field(key)

            wrapper_query = wrapper_query.select(
                *[get_reference_field(key).as_(self._suffix(key, reference_key))
                  for key in metrics.keys()]
            )

        return self._add_sorting(wrapper_query, [query.field(dkey) for dkey in dimensions.keys()])

    def _build_dimension_query(self, table, joins, dimensions, filters, limit=None):
        query = self.query_cls.from_(table).distinct()
        query = self._add_joins(joins, query)
        query = self._add_filters(query, filters, [])

        for key, dimension in dimensions.items():
            query = query.select(dimension.as_(key))

        if limit:
            query = query[:limit]

        return query

    def _build_query_inner(self, table, joins, metrics, dimensions, dfilters, mfilters, rollup):
        query = self.query_cls.from_(table)
        query = self._add_joins(joins, query)
        query = self._select_dimensions(query, dimensions, rollup)
        query = self._select_metrics(query, metrics)
        return self._add_filters(query, dfilters, mfilters)

    @staticmethod
    def _add_joins(joins, query):
        for join_table, criterion, join_type in joins:
            query = query.join(join_table, how=join_type).on(criterion)
        return query

    @staticmethod
    def _select_dimensions(query, dimensions, rollup):
        dims = [dimension.as_(key)
                for key, dimension in dimensions.items()
                if key not in chain(*rollup)]
        if dims:
            query = query.select(*dims).groupby(*dims)

        # Rollup is passed in as a list of lists so that multiple columns can be rolled up together (such as for
        # Unique dimensions)
        rollup_dims = [[dimension.as_(dimension_key)
                        for dimension_key, dimension in dimensions.items()
                        if dimension_key in keys]
                       for keys in rollup]

        # Remove entry levels
        flattened_rollup_dims = utils.flatten(rollup_dims)

        if flattened_rollup_dims:
            query = query.select(*flattened_rollup_dims).rollup(*rollup_dims)

        return query

    @staticmethod
    def _select_metrics(query, metrics):
        return query.select(*[metric.as_(key)
                              for key, metric in metrics.items()])

    @staticmethod
    def _add_filters(query, dfilters, mfilters):
        if dfilters:
            for dfx in dfilters:
                query = query.where(dfx)

        if mfilters:
            for mfx in mfilters:
                query = query.having(mfx)

        return query

    @staticmethod
    def _add_sorting(query, dimensions):
        if dimensions:
            return query.orderby(*dimensions)
        return query

    @staticmethod
    def _suffix(key, suffix):
        return '%s_%s' % (key, suffix) if suffix else key

    @staticmethod
    def _replace_filters_for_ref(dfilters, target_dimension, interval):
        """
        Replaces the dimension used by a reference in the dimension schema and dimension filter schema.

        We do this in order to build the same query with a shifted date instead of the actual date.
        """
        new_dfilters = []
        for dfilter in dfilters:
            try:
                # FIXME this is a bit hacky. Casts the fields to string to see if the filter uses this dimensinon
                # TODO provide a utility in pypika for checking if these are the same
                filter_fields = ''.join(str(f) for f in dfilter.term.fields())
                target_fields = ''.join(str(f) for f in target_dimension.fields())
                if filter_fields == target_fields:
                    dfilter = copy.deepcopy(dfilter)
                    dfilter.term = dfilter.term + interval

            except (AttributeError, IndexError):
                pass  # If the above if-expression cannot be evaluated, then its not the filter we are looking for

            new_dfilters.append(dfilter)

        return new_dfilters

    @staticmethod
    def _build_reference_join_criteria(dimension_key, dimensions, interval, query, ref_query):
        ref_join_criteria = []
        if dimension_key in dimensions:
            ref_join_criteria.append(
                query.field(dimension_key) == ref_query.field(dimension_key) + interval
            )
        ref_join_criteria += [query.field(dkey) == ref_query.field(dkey)
                              for dkey in set(dimensions.keys()) - {dimension_key}]

        # If there are no selected dimensions, this will be an empty list.
        if not ref_join_criteria:
            return None

        ref_criteria = ref_join_criteria.pop(0)
        for criteria in ref_join_criteria:
            ref_criteria &= criteria

        return ref_criteria
