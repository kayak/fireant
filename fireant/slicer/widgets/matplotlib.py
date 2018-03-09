from .base import TransformableWidget


class Matplotlib(TransformableWidget):
    def transform(self, data_frame, slicer, dimensions, references):
        raise NotImplementedError()
