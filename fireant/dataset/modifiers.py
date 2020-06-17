from pypika.terms import Term
from pypika.utils import format_alias_sql

from fireant.utils import (
    deepcopy,
    immutable,
)


class Modifier:
    wrapped_key = "wrapped"

    def __init__(self, wrapped):
        wrapped_key = super().__getattribute__("wrapped_key")
        setattr(self, wrapped_key, wrapped)

    def __getattr__(self, attr):
        if attr in ("__copy__", "__deepcopy__"):
            raise AttributeError()

        if attr in self.__dict__:
            return super().__getattribute__(attr)

        wrapped_key = super().__getattribute__("wrapped_key")
        wrapped = super().__getattribute__(wrapped_key)
        return getattr(wrapped, attr)

    def __setattr__(self, attr, value):
        wrapped_key = super().__getattribute__("wrapped_key")
        if attr == wrapped_key:
            super().__setattr__(attr, value)
            return

        wrapped = super().__getattribute__(wrapped_key)

        if attr in wrapped.__dict__:
            setattr(wrapped, attr, value)
            return

        super().__setattr__(attr, value)

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        wrapped_key = super().__getattribute__("wrapped_key")
        wrapped = super().__getattribute__(wrapped_key)
        return "{}({})".format(self.__class__.__name__, repr(wrapped))

    def __deepcopy__(self, memodict={}):
        wrapped_key = super().__getattribute__("wrapped_key")
        wrapped = super().__getattribute__(wrapped_key)
        memodict[id(wrapped)] = wrapped
        return deepcopy(self, memodict)

    @immutable
    def for_(self, wrapped):
        wrapped_key = super().__getattribute__("wrapped_key")
        setattr(self, wrapped_key, wrapped)


class FieldModifier:
    def __init__(self, field):
        self.field = field

    @immutable
    def for_(self, field):
        self.field = field

    def __deepcopy__(self, memodict={}):
        memodict[id(self.field)] = self.field
        return deepcopy(self, memodict)


class DimensionModifier(Modifier):
    """
    Base class for all dimension modifiers.
    """
    wrapped_key = "dimension"


class FilterModifier(Modifier):
    """
    Base class for all filter modifiers.
    """
    wrapped_key = "filter"

    @immutable
    def for_(self, field):
        self.filter.field = field


class RollupValue(Term):
    CONSTANT = "_FIREANT_ROLLUP_VALUE_"

    def get_sql(self, **kwargs):
        sql = f"'{self.CONSTANT}'"
        return format_alias_sql(sql, self.alias, **kwargs)


class Rollup(DimensionModifier):
    """
    A field modifier that will make totals be calculated for the wrapped dimension.
    """
    @property
    def definition(self):
        return RollupValue()


class OmitFromRollup(FilterModifier):
    """
    A filter modifier that will make the wrapped filter not apply for any total calculations, which might be
    available if an affected field has a `Rollup` dimension modifier set.
    """
    pass


class ResultSet(FilterModifier):
    """
    A filter modifier that will make the wrapped filter not filter the data. Instead, it will create new dimensions
    for representing membership of a set, given the wrapped filter's conditional.
    """
    def __init__(
        self, *args, set_label=None, complement_label=None, will_replace_referenced_dimension=True,
        will_group_complement=True, **kwargs
    ):
        """
        :param set_label: A string that will be used for naming the sub-set of the data, for which the
                          provided conditional evaluates to True (i.e the set itself). If not set,
                          a placeholder containing the conditional will be used instead.
        :param complement_label: A string that will be used for naming the sub-set of the data, for which the
                                 provided conditional evaluates to False (i.e the complement of the set). If not
                                 set, a placeholder containing the conditional will be used instead.
        :param will_replace_referenced_dimension: Whether a dimension referenced in the wrapped filter, if any,
                                                  should be replaced with the new dimension stating set membership.
                                                  If False, the wrapped filter's dimension will be kept as the
                                                  dimension right after the new set dimension. Defaults to True.
        :param will_group_complement: Whether the complement set should be broken down into its elements.
                                      Defaults to True.
        """
        super().__init__(*args, **kwargs)
        self.set_label = set_label
        self.complement_label = complement_label
        self.will_replace_referenced_dimension = will_replace_referenced_dimension
        self.will_group_complement = will_group_complement
