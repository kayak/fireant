# noinspection PyUnresolvedReferences
from .database import *
# noinspection PyUnresolvedReferences
from .slicer import *
# noinspection PyUnresolvedReferences
from .slicer.widgets import (
    DataTablesJS,
    HighCharts,
    Matplotlib,
    Pandas,
)

__version__ = '{major}.{minor}.{patch}'.format(major=1, minor=0, patch=0)
