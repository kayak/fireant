import numpy as np

from typing import Callable, List
from pypika import functions as fn
from pypika.queries import QueryBuilder

from fireant import utils
from fireant.utils import immutable
from .fields import Field
from .modifiers import FieldModifier


class ReferenceFilter:
    def __init__(self, metric: Field, operator: str, value: int):
        self.metric = metric
        self.operator = operator
        self.value = value


class Reference(FieldModifier):
    def __init__(
        self,
        field: Field,
        reference_type: 'ReferenceType',
        delta: bool = False,
        delta_percent: bool = False,
        filters: List[ReferenceFilter] = (),
    ):
        super().__init__(field)

        self.reference_type = reference_type
        self.alias = (
            reference_type.alias + "_delta_percent"
            if delta_percent
            else reference_type.alias + "_delta"
            if delta
            else reference_type.alias
        )

        self.label = (
            reference_type.label + " Δ%"
            if delta_percent
            else reference_type.label + " Δ"
            if delta
            else reference_type.label
        )

        self.time_unit = reference_type.time_unit
        self.interval = reference_type.interval

        self.delta = delta_percent or delta
        self.delta_percent = delta_percent

        self.filters = filters

    def __eq__(self, other):
        return isinstance(self, Reference) and self.field is other.field and self.alias == other.alias

    def __hash__(self):
        return hash("reference{}{}".format(self.alias, self.field))

    def __repr__(self):
        return "{}({})".format(self.alias, self.field.alias)

    @immutable
    def for_(self, field):
        self.field = field


class ReferenceType(object):
    def __init__(self, alias: str, label: str, time_unit: str, interval: int):
        self.alias = alias
        self.label = label

        self.time_unit = time_unit
        self.interval = interval

    def __call__(
        self, dimension, delta: bool = False, delta_percent: bool = False, filters: List[ReferenceFilter] = ()
    ):
        return Reference(dimension, self, delta=delta, delta_percent=delta_percent, filters=filters)

    def __eq__(self, other):
        return isinstance(self, ReferenceType) and self.time_unit == other.time_unit and self.interval == other.interval

    def __hash__(self):
        return hash("reference" + self.alias)


def _over_format(identifier: str, interval: int):
    reference_format = f'{identifier}o{identifier}'

    if interval > 1:
        reference_format = f'{identifier}o{interval}{identifier}'

    return reference_format


DaysOverDays: Callable[[int], ReferenceType] = lambda interval: ReferenceType(
    _over_format('d', interval), _over_format('D', interval), 'day', interval
)
WeeksOverWeeks: Callable[[int], ReferenceType] = lambda interval: ReferenceType(
    _over_format('w', interval), _over_format('W', interval), 'week', interval
)
MonthsOverMonths: Callable[[int], ReferenceType] = lambda interval: ReferenceType(
    _over_format('m', interval), _over_format('M', interval), 'month', interval
)
QuartersOverQuarters: Callable[[int], ReferenceType] = lambda interval: ReferenceType(
    _over_format('q', interval), _over_format('Q', interval), 'quarter', interval
)
YearsOverYears: Callable[[int], ReferenceType] = lambda interval: ReferenceType(
    _over_format('y', interval), _over_format('Y', interval), 'year', interval
)

DayOverDay = DaysOverDays(interval=1)
WeekOverWeek = WeeksOverWeeks(interval=1)
MonthOverMonth = MonthsOverMonths(interval=1)
QuarterOverQuarter = QuartersOverQuarters(interval=1)
YearOverYear = YearsOverYears(interval=1)


def reference_term(reference: Reference, original_query: QueryBuilder, ref_query: QueryBuilder):
    """
    Part of query building. Given a reference, the original dataset query, and the ref query, creates the pypika for
    the reference that should be selected in the reference container query.

    :param reference:
    :param original_query:
    :param ref_query:
    :return:
    """

    def original_field(metric):
        return original_query.field(utils.alias_selector(metric.alias))

    def ref_field(metric):
        return ref_query.field(utils.alias_selector(metric.alias))

    if reference.delta:
        if reference.delta_percent:
            return lambda metric: (original_field(metric) - ref_field(metric)) * (100 / fn.NullIf(ref_field(metric), 0))

        return lambda metric: original_field(metric) - ref_field(metric)

    return ref_field


def calculate_delta_percent(ref_df, ref_delta_df):
    # pandas raises an exception when dividing by zero
    return 100.0 * ref_delta_df.divide(ref_df.replace(0, np.nan))
