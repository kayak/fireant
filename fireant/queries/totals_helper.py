from fireant.dataset.totals import Rollup
from .finders import find_filters_for_totals


def adapt_for_totals_query(totals_dimension, dimensions, filters):
    """
    Adapt filters for totals query. This function will select filters for total dimensions depending on the
    apply_filter_to_totals values for the filters. A total dimension with value None indicates the base query for
    which all filters will be applied by default.

    :param totals_dimension:
    :param dimensions:
    :param filters:
    :return:
    """
    is_totals_query = totals_dimension is not None
    raw_dimensions = [dimension.dimension
                      if isinstance(dimension, Rollup)
                      else dimension
                      for dimension in dimensions]

    if not is_totals_query:
        return raw_dimensions, filters

    # Get an index to split the dimensions before and after the totals dimension
    index = [i
             for i, dimension in enumerate(dimensions)
             if dimension is totals_dimension][0]
    totals_dims = [Rollup(dimension)
                   if i >= index
                   else dimension
                   for i, dimension in enumerate(raw_dimensions)]
    totals_filters = find_filters_for_totals(filters)

    return totals_dims, totals_filters
