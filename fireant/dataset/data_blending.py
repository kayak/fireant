from fireant import (
    Field,
)
from fireant.dataset.klass import (
    DataSet,
)
from fireant.queries.builder import (
    DataSetBlenderQueryBuilder,
    DimensionChoicesQueryBuilder,
    DimensionLatestQueryBuilder,
)
from fireant.utils import immutable


class DataSetBlender(object):
    """
    The DataSetBlender class is the DataSet equivalent for implementing data blending, across distinct DataSet
    instances.
    """

    def __init__(self, primary_dataset):
        """
        Constructor for a data blended slicer.  Contains all the fields to initialize the slicer.

        :param primary_dataset: (Required)
            The primary dataset, which table will be used as part of the FROM expression.
        """
        self.primary_dataset = primary_dataset
        self.secondary_datasets = []
        self.fields = DataSet.Fields()

        # This is for mapping fields across datasets. By default, if not provided, matches will be by primary
        # dataset and equivalent secondary dataset's field aliases, but DataSetBlender users can override that too
        # when joining datasets. See join method for more info.
        self.field_mapping = {}

        self.__primary_dataset_fields_per_alias = {}
        primary_dataset_field_mapping = {}

        for primary_field in self.primary_dataset.fields:
            self.__primary_dataset_fields_per_alias[primary_field.alias] = primary_field
            primary_dataset_field_mapping[primary_field] = primary_field
            self.fields.append(primary_field)

        self.field_mapping[primary_dataset.table] = primary_dataset_field_mapping

        # add query builder entry points
        self.query = DataSetBlenderQueryBuilder(self)
        self.latest = DimensionLatestQueryBuilder(self.primary_dataset)

    def __eq__(self, other):
        return isinstance(other, DataSetBlender) \
               and self.fields == other.fields

    def __repr__(self):
        return 'BlendedSlicer(fields=[{}])' \
            .format(','.join([repr(f)
                              for f in self.fields]))

    def __hash__(self):
        return hash((
            self.table, self.primary_dataset.database, tuple(self.secondary_datasets), self.joins, self.fields,
        ))

    @property
    def table(self):
        return self.primary_dataset.table

    @immutable
    def join(self, secondary_dataset, field_mapping=None):
        """
        Adds a join when building a slicer query. The join type is left outer by default.

        :Example:

            # Fields will match by alias
            my_dataset_blender_instance_with_fields.join(my_secondary_dataset)

            # Fields will match as specified in the provided field_mapping
            my_dataset_blender_instance_with_fields.join(my_secondary_dataset, field_mapping={
                my_primary_dataset.fields['fieldA']: my_secondary_dataset.fields['field_a'],
                my_primary_dataset.fields['fieldB']: my_secondary_dataset.fields['b_field'],
            })

        :param secondary_dataset: (Required)
            The secondary dataset, which table will be used as part of the one or more JOIN expressions.
        :param field_mapping: (Optional)
            A dictionary with primary dataset's fields as keys and the provided secondary dataset's fields as values.
            If not provided, fields with the same alias will be matched by default.

        :return:
            A copy of this DataSetBlender instance with the join added.
        """
        if field_mapping is None:
            field_mapping = {}

            for secondary_field in secondary_dataset.fields:
                if secondary_field.alias in self.__primary_dataset_fields_per_alias:
                    primary_field = self.__primary_dataset_fields_per_alias[secondary_field.alias]
                    field_mapping[primary_field] = secondary_field

        self.field_mapping[secondary_dataset.table] = field_mapping

        for secondary_field in secondary_dataset.fields:
            if secondary_field.alias not in self.fields:
                self.fields.append(secondary_field)

        self.secondary_datasets.append(secondary_dataset)

    @immutable
    def field(self, *args, field_class=Field, **kwargs):
        """
        Adds a field when building a slicer query. Fields are similar to a column in a database query result set.

        :param field_class: (Optional)
            A class that inherits from Field. That will be used for instantiating a new field with the provided
            args and kwargs. Defaults to Field.
        :return:
            A copy of this DataSetBlender instance with the field added.
        """
        field = field_class(self, *args, **kwargs)

        if not field.definition.is_aggregate:
            field.choices = DimensionChoicesQueryBuilder(self.primary_dataset, field)

        self.fields.append(field)
