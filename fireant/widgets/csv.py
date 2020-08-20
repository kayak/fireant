from _csv import QUOTE_MINIMAL
from typing import Iterable

from fireant.dataset.fields import Field
from .pandas import Pandas


class CSV(Pandas):
    def __init__(
        self, metric: Field, *metrics: Iterable[Field], group_pagination=False, **kwargs
    ):
        super().__init__(metric, *metrics, **kwargs)
        self.group_pagination = group_pagination

    def transform(
        self,
        data_frame,
        dimensions,
        references,
        annotation_frame=None,
        use_raw_values=None,
    ):
        result_df = super(CSV, self).transform(
            data_frame, dimensions, references, use_raw_values=True
        )
        # Unset the column level names because they're a bit confusing in a csv file
        result_df.columns.names = [None] * len(result_df.columns.names)
        return result_df.to_csv(na_rep="", quoting=QUOTE_MINIMAL)
