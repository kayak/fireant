class Annotation(object):
    def __init__(self, table, field, alignment_field, dataset_alignment_field_alias):
        """
        :param table:
            A Pypika instance of the annotation table.
        :param field:
            The annotation field.
        :param alignment_field:
            An additional field in the annotation table for aligning the annotation data with the dataset.
        :param dataset_alignment_field_alias:
            An alias of the alignment dimension in the associated dataset.
        """
        self.table = table
        self.field = field
        self.alignment_field = alignment_field
        self.dataset_alignment_field_alias = dataset_alignment_field_alias
