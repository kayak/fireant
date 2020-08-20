from typing import Union

import pandas as pd

from fireant.dataset.fields import Field
from fireant.dataset.operations import Operation
from fireant.dataset.references import Reference
from fireant.exceptions import DataSetException
from fireant.reference_helpers import (
    reference_alias,
    reference_label,
    reference_prefix,
    reference_suffix,
)
from fireant.utils import (
    alias_selector,
    deepcopy,
    immutable,
)


class MetricRequiredException(DataSetException):
    pass


class Widget:
    def __init__(self, *items: Union[Field, Operation]):
        self.items = list(items)

    @immutable
    def item(self, item):
        self.items.append(item)

    @property
    def metrics(self):
        if 0 == len(self.items):
            raise MetricRequiredException(str(self))

        return [
            metric
            for group in self.items
            for metric in getattr(group, "metrics", [group])
        ]

    def __deepcopy__(self, memodict={}):
        for item in self.items:
            memodict[id(item)] = item
        return deepcopy(self, memodict)

    @property
    def operations(self):
        return [item for item in self.items if isinstance(item, Operation)]

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.items == other.items

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__, ",".join(str(m) for m in self.items)
        )


class TransformableWidget(Widget):
    # This attribute can be overridden in order to sort groups. Useful in cases like for charts where sorting
    # should be applied to the number of series rather than the number of data points.
    group_pagination = False

    @staticmethod
    def hide_data_frame_indexes(data_frame, dimensions_to_hide):
        data_frame_indexes = (
            [data_frame.index.name] if not isinstance(data_frame.index, pd.MultiIndex) else data_frame.index.names
        )

        for dimension in dimensions_to_hide:
            dimesion_alias = alias_selector(dimension.alias)
            if dimesion_alias in data_frame_indexes:
                data_frame.reset_index(level=dimesion_alias, drop=True, inplace=True)

    def transform(self, data_frame, dimensions, references, annotation_frame=None):
        """
        - Main entry point -

        Transformers the result set `pd.DataFrame` from a dataset query into the output format for this specific widget
        type.

        :param data_frame:
            The data frame containing the data. Index must match the dimensions parameter.
        :param dimensions:
            A list of dimensions that are being rendered.
        :param references:
            A list of references that are being rendered.
        :param annotation_frame:
            A data frame containing the annotation data.
        :return:
            A dict meant to be dumped as JSON.
        """
        raise NotImplementedError()


class ReferenceItem:
    def __init__(self, item, reference):
        assert isinstance(reference, Reference)
        self.data_type = item.data_type
        self.alias = reference_alias(item, reference)
        self.label = reference_label(item, reference)
        self.prefix = reference_prefix(item, reference)
        self.suffix = reference_suffix(item, reference)
        self.thousands = item.thousands
        self.precision = item.precision
