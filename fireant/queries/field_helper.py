from fireant.dataset.intervals import DatetimeInterval
from fireant.utils import alias_selector


def make_term_for_metrics(metric):
    return metric.definition.as_(alias_selector(metric.alias))


def make_term_for_dimension(dimension, window=None):
    """
    Makes a list of pypika terms for a given slicer definition.

    :param dimension:
        A slicer dimension.
    :param window:
        A window function to apply to the dimension definition if it is a continuous dimension.
    :return:
        a list of terms required to select and group by in a SQL query given a slicer dimension. This list will contain
        either one or two elements. A second element will be included if the dimension has a definition for its display
        field.
    """
    f_alias = alias_selector(dimension.alias)

    if window and isinstance(dimension, DatetimeInterval):
        return window(dimension.definition, dimension.interval_key).as_(f_alias)

    return dimension.definition.as_(f_alias)


def make_orders_for_dimensions(dimensions):
    """
    Creates a list of ordering for a slicer query based on a list of dimensions. The dimensions's display definition is
    used preferably as the ordering term but the definition is used for dimensions that do not have a display
    definition.

    :param dimensions:
    :return:
        a list of tuple pairs like (term, orientation) for ordering a SQL query where the first element is the term
        to order by and the second is the orientation of the ordering, ASC or DESC.
    """

    # Use the same function to make the definition terms to force it to be consistent.
    # Always take the last element in order to prefer the display definition.
    definitions = [make_term_for_dimension(dimension)
                   for dimension in dimensions]

    return [(definition, None)
            for definition in definitions]
