# coding: utf-8
import copy
import logging

import numpy as np
import pandas as pd

from fireant.slicer.operations import Totals
from pypika import Query, JoinType

logger = logging.Logger('fireant')


class QueryManager(object):
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

            If a dict is used instead of an OrderedDict, then the index levels cannot be guarenteed, so in most cases an
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
            A pd.DataFrame indexed by the provided dimensions paramaters containing columns for each metrics parameter.
        """
        query = self._build_data_query(table, joins or dict(), metrics or dict(), dimensions or dict(),
                                       dfilters or dict(), mfilters or dict(), references or dict(), rollup or list())

        querystring = str(query)
        logger.info("Executing query:\n----START----\n{query}\n-----END-----".format(query=querystring))

        dataframe = database.fetch_dataframe(querystring)

        for dimension_key in rollup:
            dataframe[dimension_key].replace([np.nan], [Totals.key], inplace=True)

        dataframe.columns = [col.decode('utf-8') if isinstance(col, bytes) else col
                             for col in dataframe.columns]

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

    def _build_data_query(self, table, joins, metrics, dimensions, dfilters, mfilters, references, rollup):
        args = (table, joins, metrics, dimensions, dfilters, mfilters, rollup)
        query = self._build_query_inner(*args)

        if references:
            return self._build_reference_query(query, references, *args)

        return self._add_sorting(query, list(dimensions.values()))

    def _build_reference_query(self, query, references, table, joins, metrics, dimensions, dfilters, mfilters, rollup):
        wrapper_query = Query.from_(query).select(*[query.field(key).as_(key)
                                                    for key in list(dimensions.keys()) + list(metrics.keys())])

        for reference_key, schema in references.items():
            dimension_key = schema['dimension']

            dimensions, dfilters = self._replace_dim_for_ref(dfilters, dimension_key, dimensions, schema['interval'])
            ref_query = self._build_query_inner(table, joins, metrics, dimensions, dfilters, mfilters, rollup)

            ref_criteria = query.field(dimension_key) == ref_query.field(dimension_key)
            for dkey in set(dimensions.keys()) - {dimension_key}:
                ref_criteria &= query.field(dkey) == ref_query.field(dkey)

            # Optional modifier function to modify the metric in the reference query. This is for delta and delta
            # percentage references. It is None for normal references and this default should be used
            modifier = schema.get('modifier')

            reference_metrics = [
                modifier(query.field(key), ref_query.field(key)).as_(self._suffix(key, reference_key))
                for key in metrics.keys()
                ] if modifier else [
                ref_query.field(key).as_(self._suffix(key, reference_key))
                for key in metrics.keys()]

            # Join the reference query and select the reference dimension and all metrics
            # This ignores additional dimensions since they are identical to the primary results
            wrapper_query = (
                wrapper_query.join(ref_query, JoinType.left).on(ref_criteria)
                    # Don't select this for now
                    # .select(ref_dimension.as_(self._suffix(dimension_key, reference_key)))
                    .select(*[
                    # Select the metrics after applying the modifier (such as Delta/Delta Percentage) if there is one
                    modifier(query.field(key), ref_query.field(key)).as_(self._suffix(key, reference_key))
                    for key in metrics.keys()
                    ] if modifier else [
                    # If no modifier, just select the metric from the subquery
                    ref_query.field(key).as_(self._suffix(key, reference_key))
                    for key in metrics.keys()])
            )

        return self._add_sorting(wrapper_query, [query.field(dkey) for dkey in dimensions.keys()])

    def _build_dimension_query(self, table, joins, dimensions, filters, limit=None):
        query = Query.from_(table).distinct()
        query = self._add_joins(joins, query)
        query = self._add_filters(query, filters, [])

        for key, dimension in dimensions.items():
            query = query.select(dimension.as_(key))

        if limit:
            query = query[:limit]

        return query

    def _build_query_inner(self, table, joins, metrics, dimensions, dfilters, mfilters, rollup):
        query = Query.from_(table)
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
                if key not in rollup]
        if dims:
            query = query.select(*dims).groupby(*dims)

        rollup_dims = [dimension.as_(key)
                       for key, dimension in dimensions.items()
                       if key in rollup]
        if rollup_dims:
            query = query.select(*rollup_dims).rollup(*rollup_dims)

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
    def _replace_dim_for_ref(dfilters, dimension_key, dimensions, interval):
        """
        Replaces the dimension used by a reference in the dimension schema and dimension filter schema.

        We do this in order to build the same query with a shifted date instead of the actual date.
        """
        target_dimension = dimensions[dimension_key]

        new_dimensions = copy.deepcopy(dimensions)
        new_dimensions[dimension_key] = target_dimension + interval

        new_dfilters = []
        for dfilter in dfilters:
            # This is a very hack way of doing this.  This expects the dfilter to use the exact same reference to
            # the dimension's definition, which the slicer manager guarentees and the unit tests simulate.
            # TODO provide a utility in pypika for checking if these are the same
            try:
                if dfilter.term.fields()[0] is target_dimension.fields()[0]:
                    dfilter = copy.deepcopy(dfilter)
                    dfilter.term = dfilter.term + interval

            except (AttributeError, IndexError):
                pass  # If the above if-expression cannot be evaluated, then its not the filter we are looking for

            new_dfilters.append(dfilter)

        return new_dimensions, new_dfilters
