from .base import Widget


class Matplotlib(Widget):
    def transform(self, data_frame, slicer, dimensions):
        raise NotImplementedError()
