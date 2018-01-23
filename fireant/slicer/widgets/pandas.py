from .base import Widget


class Pandas(Widget):
    def transform(self, data_frame, slicer, dimensions):
        raise NotImplementedError()
