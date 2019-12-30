from fireant.dataset.intervals import DatetimeInterval
from fireant.utils import alias_selector


def make_term_for_metrics(metric):
    f_alias = alias_selector(metric.alias)
    return metric.definition.as_(f_alias)


def make_term_for_dimension(dimension, window=None):
    """
    Makes a list of pypika terms for a given dataset definition.

    :param dimension:
        A dataset dimension.
    :param window:
        A window function to apply to the dimension definition if it is a continuous dimension.
    :return:
        a list of terms required to select and group by in a SQL query given a dataset dimension. This list will contain
        either one or two elements. A second element will be included if the dimension has a definition for its display
        field.
    """
    f_alias = alias_selector(dimension.alias)

    if window and isinstance(dimension, DatetimeInterval):
        return window(dimension.definition, dimension.interval_key).as_(f_alias)

    return dimension.definition.as_(f_alias)


def initialize_orders(orders, dimensions):
    """
    Initialize the DataSetQueryBuilder values for orders so that the SQL queries can be built. This also initializes
    the default values for orders, which is all of the dimensions, if no order is specified.
    """
    if orders is not None:
        return [
            (field.definition.as_(alias_selector(field.alias)), orientation)
            for (field, orientation) in orders
        ]

    # Initialize ordering to be by all dimensions

    # Use the same function to make the definition terms to force it to be consistent.
    # Always take the last element in order to prefer the display definition.
    definitions = [make_term_for_dimension(dimension) for dimension in dimensions]

    return [(definition, None) for definition in definitions]
