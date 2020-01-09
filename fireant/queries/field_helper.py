from fireant.dataset.intervals import DatetimeInterval
from fireant.utils import alias_selector


def make_term_for_field(field, window=None):
    """
    Makes a list of pypika terms for a given dataset field.

    :param field:
        A field from a dataset.
    :param window:
        A window function to apply to the dimension definition if it is a continuous dimension.
    :return:
        a list of terms required to select and group by in a SQL query given a dataset dimension. This list will contain
        either one or two elements. A second element will be included if the dimension has a definition for its display
        field.
    """
    f_alias = alias_selector(field.alias)

    if window and isinstance(field, DatetimeInterval):
        return window(field.definition, field.interval_key).as_(f_alias)

    return field.definition.as_(f_alias)
