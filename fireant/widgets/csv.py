import pandas as pd

from _csv import QUOTE_MINIMAL
from typing import Iterable, List, Optional

from fireant.dataset.fields import Field
from fireant.dataset.references import Reference
from .pandas import Pandas


class CSV(Pandas):
    def __init__(self, metric: Field, *metrics: Iterable[Field], group_pagination: bool = False, **kwargs):
        super().__init__(metric, *metrics, **kwargs)
        self.group_pagination = group_pagination

    def transform(
        self,
        data_frame: pd.DataFrame,
        dimensions: List[Field],
        references: List[Reference],
        annotation_frame: Optional[pd.DataFrame] = None,
        use_raw_values: bool = None,
    ):
        result_df = super(CSV, self).transform(data_frame, dimensions, references, use_raw_values=True)
        # Unset the column level names because they're a bit confusing in a csv file
        result_df.columns.names = [None] * len(result_df.columns.names)
        return result_df.to_csv(na_rep="", quoting=QUOTE_MINIMAL)
