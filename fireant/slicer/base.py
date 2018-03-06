class SlicerElement(object):
    """
    The `SlicerElement` class represents an element of the slicer, either a metric or dimension, which contains
    information about such as how to query it from the database.
    """
    def __init__(self, key, label=None, definition=None, display_definition=None):
        """
        :param key:
            The unique identifier of the slicer element, used in the Slicer manager API to reference a defined element.

        :param label:
            A displayable representation of the column.  Defaults to the key capitalized.

        :param definition:
            The definition of the element as a PyPika expression which defines how to query it from the database.

        :param display_definition:
            The definition of the display field for the element as a PyPika expression. Similar to the definition except
            used for querying labels.
        """
        self.key = key
        self.label = label or key
        self.definition = definition

        self.display_definition = display_definition

        self.display_key = '{}_display'.format(key) \
            if display_definition is not None \
            else None

    def __repr__(self):
        return self.key

    @property
    def has_display_field(self):
        return getattr(self, 'display_definition', None) is not None

    def __hash__(self):
        return hash('{}({})'.format(self.__class__.__name__, self.definition))
