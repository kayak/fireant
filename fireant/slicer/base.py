class SlicerElement(object):
    """
    The `SlicerElement` class represents an element of the slicer, either a metric or dimension, which contains
    information about such as how to query it from the database.
    """
    def __init__(self, key, label=None, definition=None):
        """
        :param key:
            The unique identifier of the slicer element, used in the Slicer manager API to reference a defined element.

        :param label:
            A displayable representation of the column.  Defaults to the key capitalized.

        :param definition:
            The definition of the element as a PyPika expression which defines how to query it from the database.
        """
        self.key = key
        self.label = label or key
        self.definition = definition

    def __repr__(self):
        return self.key

    @property
    def has_display_field(self):
        return getattr(self, 'display_definition', None) is not None

    def __hash__(self):
        return hash(self.__class__.__name__ + self.key)
