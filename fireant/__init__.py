from .database import *
from .dataset.data_blending import DataSetBlender
from .dataset.fields import (
    DataSetFilterException,
    DataType,
    Field,
)
from .dataset.intervals import (
    NumericInterval,
    day,
    hour,
    month,
    quarter,
    week,
    year,
)
from .dataset.joins import Join
from .dataset.klass import DataSet
from .dataset.modifiers import (
    OmitFromRollup,
    Rollup,
    ResultSet,
)
from .dataset.operations import (
    CumMean,
    CumProd,
    CumSum,
    Operation,
    RollingMean,
    Share,
)
from .dataset.references import (
    DayOverDay,
    MonthOverMonth,
    QuarterOverQuarter,
    WeekOverWeek,
    YearOverYear,
    DaysOverDays,
    WeeksOverWeeks,
    MonthsOverMonths,
    QuartersOverQuarters,
    YearsOverYears,
)
from .exceptions import DataSetException
from .widgets import *

from pypika.terms import Term


# Monkey patching PyPika's Term class to use the old hash functionality
def __hash__(self) -> int:
    return hash(self.get_sql(with_alias=True))


Term.__hash__ = __hash__


__version__ = "7.9.0"
