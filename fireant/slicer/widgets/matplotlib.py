from .base import (
    Widget,
    TransformableWidget,
)


class Matplotlib(TransformableWidget):
    def transform(self, data_frame, slicer, dimensions):
        raise NotImplementedError()
