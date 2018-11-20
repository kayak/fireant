from typing import Union

from fireant import (
    Metric,
    Operation,
)
from fireant.slicer.exceptions import MetricRequiredException
from fireant.utils import immutable


class Widget:
    def __init__(self, *items: Union[Metric, Operation]):
        self.items = list(items)

    @immutable
    def item(self, item):
        self.items.append(item)

    @property
    def metrics(self):
        if 0 == len(self.items):
            raise MetricRequiredException(str(self))

        return [metric
                for group in self.items
                for metric in getattr(group, 'metrics', [group])]

    @property
    def operations(self):
        return [item
                for item in self.items
                if isinstance(item, Operation)]

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
               and self.items == other.items

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               ','.join(str(m) for m in self.items))


class TransformableWidget(Widget):
    # This attribute can be overridden in order to paginate in groups. Useful in cases like for charts where pagination
    # should be applied to the number of series rather than the number of data points.
    group_pagination = False

    def transform(self, data_frame, slicer, dimensions, references):
        """
        - Main entry point -

        Transformers the result set `pd.DataFrame` from a slicer query into the output format for this specific widget
        type.

        :param data_frame:
            The data frame containing the data. Index must match the dimensions parameter.
        :param slicer:
            The slicer that is in use.
        :param dimensions:
            A list of dimensions that are being rendered.
        :param references:
            A list of references that are being rendered.
        :return:
            A dict meant to be dumped as JSON.
        """
        raise NotImplementedError()
